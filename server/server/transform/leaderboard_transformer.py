import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database
import pytz
import schedule

from server.const import Collection, ZERO_DECIMAL128, ZERO_DECIMAL
from server.graphql.resolvers.helpers import convert_timestamp_to_datetime
from server.transform.lp_contest_updates import (
    insert_lp_leaderboard_snapshot,
    get_current_position_total_fees_usd,
    get_time_vested_value, get_pool_boost,
    process_position_for_lp_leaderboard
)

from structlog import get_logger


logger = get_logger(__name__)


EARLY_ADOPTER_MULTIPLIER_CONST = 3
LAST_SWAPS_PERIOD_IN_HOURS = 24
SWAP_PERCENTILE_TRESHOLD = 0.75


async def yield_snapshot_records(db: Database, collection: str) -> dict:
    pipeline = [
        {
            '$match': {
                'processed': False,
            }
        },
        {
            '$sort': {
                'timestamp': 1,
            }
        },
    ]
    for record in db[collection].aggregate(pipeline):
        yield record


async def yield_position_records(db: Database) -> dict:
    query = {
        'liquidity': {'$ne': 0},
    }
    for record in db[Collection.POSITIONS].find(query):
        yield record


async def handle_positions_for_lp_leaderboard(db: Database, rpc_url: str):
    # ensure that the transformer runs after 00:00
    await asyncio.sleep(1)

    processed_positions_records = 0
    current_dt = datetime.now(timezone.utc)
    current_dt = current_dt - timedelta(days=1)
    current_dt = current_dt.replace(hour=23, minute=59, second=59)
    
    async for position_record in yield_position_records(db):
        last_updated_dt = convert_timestamp_to_datetime(position_record['lastUpdatedTimestamp'])
        last_updated_dt = last_updated_dt.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=timezone.utc)

        missing_block, records_to_be_inserted = await process_position_for_lp_leaderboard(
            db, current_dt, last_updated_dt, position_record)
        
        if not missing_block:
            for record in records_to_be_inserted:
                await insert_lp_leaderboard_snapshot(record['event_data'], db, position_record=record['position_record'])
            processed_positions_records += 1

    logger.info(f'Successfully processed {processed_positions_records} Positions records for the leaderboard contest')


async def calculate_lp_leaderboard_user_total_points(db: Database, rpc_url: str):
    processed_lp_records = 0

    async for record in yield_snapshot_records(db, Collection.LP_LEADERBOARD_SNAPSHOT):
        position_record = record['position']
        current_position_total_fees_usd = await get_current_position_total_fees_usd(record, position_record, db, rpc_url)
        total_fees_usd = position_record['totalFeesUSD'].to_decimal()
        current_fees_usd = current_position_total_fees_usd - total_fees_usd
        if current_fees_usd < ZERO_DECIMAL:
            current_fees_usd = ZERO_DECIMAL

        last_time_vested_value, current_time_vested_value, period = await get_time_vested_value(
            db, record, position_record)

        pool_boost = await get_pool_boost(position_record['token0Address'], position_record['token1Address'])

        points = current_fees_usd * last_time_vested_value * pool_boost * Decimal(1000)

        position_update_data = dict()
        position_update_data['$set'] = dict()
        position_update_data['$inc'] = dict()
        position_update_data['$set']['totalFeesUSD'] = Decimal128(current_position_total_fees_usd)
        position_update_data['$set']['timeVestedValue'] = Decimal128(current_time_vested_value)
        position_update_data['$set']['lastUpdatedTimestamp'] = record['timestamp']
        position_update_data['$inc']['lpPoints'] = Decimal128(points)

        position_query = {'positionId': position_record['positionId']}
        db[Collection.POSITIONS].update_one(position_query, position_update_data)

        lp_contest_snapshot_data = dict()
        lp_contest_snapshot_data['$set'] = dict()
        lp_contest_snapshot_data['$set']['currentFeesUsd'] = Decimal128(current_fees_usd)
        lp_contest_snapshot_data['$set']['lastTimeVestedValue'] = Decimal128(last_time_vested_value)
        lp_contest_snapshot_data['$set']['currentTimeVestedValue'] = Decimal128(current_time_vested_value)
        lp_contest_snapshot_data['$set']['period'] = period
        lp_contest_snapshot_data['$set']['poolBoost'] = Decimal128(pool_boost)
        lp_contest_snapshot_data['$set']['lpPoints'] = Decimal128(points)
        lp_contest_snapshot_data['$set']['processed'] = True

        lp_contest_snapshot_query = {'_id': record['_id']}
        db[Collection.LP_LEADERBOARD_SNAPSHOT].update_one(lp_contest_snapshot_query, lp_contest_snapshot_data)

        lp_contest_data = dict()
        lp_contest_data['$inc'] = dict()
        lp_contest_data['$inc']['points'] = Decimal128(points)

        lp_contest_query = {'userAddress': position_record['ownerAddress']}
        db[Collection.LP_LEADERBOARD].update_one(lp_contest_query, lp_contest_data, upsert=True)

        processed_lp_records += 1
        if processed_lp_records % 100 == 0:
            logger.info(f'Processed {processed_lp_records} records')

    logger.info(f'Successfully calculated {processed_lp_records} user records for the lp leaderboard contest')


async def insert_volume_leaderboard_snapshot(db: Database, fess_usd: Decimal, record: dict):
    volume_leaderboard_snapshot_record = {
        'userAddress': record['tx_sender'],
        'swapFeesUsd': Decimal128(fess_usd),
        'sybilMultiplier': 0,
        'earlyMultiplier': EARLY_ADOPTER_MULTIPLIER_CONST,
        'timestamp': record['timestamp'],
        'volumePoints': ZERO_DECIMAL128,
        'processed': False,
    }
    db[Collection.VOLUME_LEADERBOARD_SNAPSHOT].insert_one(volume_leaderboard_snapshot_record)


async def calculate_volume_leaderboard_user_total_points(db: Database):
    processed_volume_records = 0
    volume_contest_update_operation = []
    volume_contest_snapshot_update_operation = []

    dt = datetime.now() - timedelta(hours=LAST_SWAPS_PERIOD_IN_HOURS)
    timestamp_period = int(dt.timestamp() * 1000)

    pipeline = [
        {
            '$match': {
                'timestamp': {'$gte': timestamp_period},
            }
        },
        {
            '$sort': {
                'swapFeesUsd': 1,
            }
        },
    ]
    results = db[Collection.VOLUME_LEADERBOARD_SNAPSHOT].aggregate(pipeline)
    results = list(results)
    last_swap_event = None
    if results:
        last_index = int(len(results) * SWAP_PERCENTILE_TRESHOLD) - 1
        last_swap_event = results[last_index]
    
    async for record in yield_snapshot_records(db, Collection.VOLUME_LEADERBOARD_SNAPSHOT):
        fees_usd = record['swapFeesUsd'].to_decimal()
        sybil_multiplier = 1
        if last_swap_event and last_swap_event['swapFeesUsd'].to_decimal() > fees_usd:
            sybil_multiplier = 0

        points = fees_usd * EARLY_ADOPTER_MULTIPLIER_CONST * sybil_multiplier * 1000
        
        volume_contest_snapshot_update_operation.append(
            UpdateOne(
                {'_id': record['_id']},
                {'$set': {
                    'sybilMultiplier': sybil_multiplier,
                    'volumePoints': Decimal128(points),
                    'processed': True,
                }}
            ))

        volume_contest_update_operation.append(
            UpdateOne(
                {'userAddress': record['userAddress']},
                {'$inc': {'points': Decimal128(points)}},
                upsert=True
            ))
        processed_volume_records += 1
        
    if volume_contest_snapshot_update_operation:
        db[Collection.VOLUME_LEADERBOARD_SNAPSHOT].bulk_write(volume_contest_snapshot_update_operation)

    if volume_contest_update_operation:
        db[Collection.VOLUME_LEADERBOARD].bulk_write(volume_contest_update_operation)
    logger.info(f'Successfully calculated {processed_volume_records} user records for the volume leaderboard contest')


async def process_leaderboard(mongo_url: str, mongo_database: Database, rpc_url: str):
    logger.info('Leaderboard transformer started...')
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        await handle_positions_for_lp_leaderboard(db, rpc_url)
        await calculate_lp_leaderboard_user_total_points(db, rpc_url)
        await calculate_volume_leaderboard_user_total_points(db)


def schedule_process_leaderboard(mongo_url: str, mongo_database: Database, rpc_url: str):
    asyncio.ensure_future(process_leaderboard(mongo_url, mongo_database, rpc_url))

def process_leaderboard_once(mongo_url: str, mongo_database: Database, rpc_url: str):
    asyncio.ensure_future(process_leaderboard(mongo_url, mongo_database, rpc_url))
    return schedule.CancelJob


async def run_leaderboard_transformer(mongo_url: str, mongo_database: str, rpc_url: str):
    schedule.every().day.at('00:00', pytz.timezone('UTC')).do(schedule_process_leaderboard, 
                                                              mongo_url=mongo_url, 
                                                              mongo_database=mongo_database, 
                                                              rpc_url=rpc_url)
    schedule.every(10).minutes.do(process_leaderboard_once, 
                                    mongo_url=mongo_url, 
                                    mongo_database=mongo_database, 
                                    rpc_url=rpc_url)
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)
