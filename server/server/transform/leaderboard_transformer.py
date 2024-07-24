import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from bson import Decimal128
from pymongo import MongoClient, UpdateOne, ASCENDING
from pymongo.database import Database
import pytz
import schedule

from server.const import Collection, ZERO_DECIMAL128, ZERO_DECIMAL
from server.graphql.resolvers.helpers import convert_timestamp_to_datetime
from server.transform.lp_contest_updates import (
    insert_lp_leaderboard_snapshot,
    get_current_position_total_fees_usd,
    get_time_vested_value, get_pool_boost,
    process_position_for_lp_leaderboard,
    simulate_collect_tx,
)
from server.query_utils import get_position_record, get_teahouse_position_record, simple_call

from structlog import get_logger


logger = get_logger(__name__)


EARLY_ADOPTER_MULTIPLIER_CONST = 3
LAST_SWAPS_PERIOD_IN_HOURS = 24
SWAP_PERCENTILE_TRESHOLD = 0.75


class NftPosition:
    collection = ''
    log_msg = ''
    lp_snapshot_query = {}
    lp_contest_user_key = 'ownerAddress'
    teahouse = False

    @staticmethod
    async def get_latest_position(*args, **kwargs) -> dict:
        pass 

    @staticmethod
    async def update_position(db: Database, position_record: dict, position_update_data: dict):
        pass

    @staticmethod
    async def get_uncollected_fees(rpc_url: str, position_record: dict, block: int) -> tuple[Decimal, Decimal]:
        pass


class JediSwapPosition(NftPosition):
    collection = Collection.POSITIONS
    lp_snapshot_query = {
        '$and': [
            {
                'positionId': {
                    '$ne': ''
                }
            },
            {'processed': False}
        ]
    }

    @staticmethod
    async def get_latest_position(*args, **kwargs) -> dict:
        db = kwargs['db']
        record = kwargs['record']
        return await get_position_record(db, record['positionId'])

    @staticmethod
    async def update_position(db: Database, position_record: dict, position_update_data: dict):
        position_query = {'positionId': position_record['positionId']}
        db[JediSwapPosition.collection].update_one(position_query, position_update_data)
    
    @staticmethod
    async def get_uncollected_fees(rpc_url: str, position_record: dict, block: int) -> tuple[Decimal, Decimal]:
        return await simulate_collect_tx(rpc_url, position_record, block)


class TeahousePosition(NftPosition):
    collection = Collection.TEAHOUSE_VAULT
    log_msg = 'teahouse '
    lp_snapshot_query = {
        '$and': [
            {
                '$or': [
                    {'positionId': None},
                    {'positionId': ''}
                ]
            },
            {'processed': False}
        ]
    }
    lp_contest_user_key = 'vaultAddress'
    teahouse = True

    @staticmethod
    async def get_latest_position(*args, **kwargs) -> dict:
        db = kwargs['db']
        record = kwargs['record']
        rpc_url = kwargs['rpc_url']
        return await get_teahouse_position_record(db, record['position'], rpc_url)

    @staticmethod
    async def update_position(db: Database, position_record: dict, position_update_data: dict):
        position_query = {
            'poolAddress': position_record['poolAddress'],
        }
        db[TeahousePosition.collection].update_one(position_query, position_update_data)

    @staticmethod
    async def get_uncollected_fees(rpc_url: str, position_record: dict, block: int) -> tuple[Decimal, Decimal]:
        try:
            try:
                result = await simple_call(position_record['vaultAddress'], 'all_position_info', [], rpc_url, block_number=block)
            except Exception:
                logger.warn('Trying to call the previous block...')
                result = await simple_call(position_record['vaultAddress'], 'all_position_info', [], rpc_url, block_number=block-1)
                logger.info('Success!')
            return result[-4], result[-2]
        except Exception:
            logger.warn(f"Couldn't fetch fees from Teahouse position {position_record['poolAddress']} for {block} block")
            return ZERO_DECIMAL, ZERO_DECIMAL


async def yield_position_records(db: Database, collection: str) -> dict:
    query = {
        'liquidity': {'$ne': ZERO_DECIMAL128},
    }
    for record in db[collection].find(query):
        yield record


async def handle_positions_for_lp_leaderboard(db: Database, position_class: NftPosition):
    # ensure that the transformer runs after 00:00
    await asyncio.sleep(1)

    processed_positions_records = 0
    current_dt = datetime.now(timezone.utc)
    current_dt = current_dt - timedelta(days=1)
    current_dt = current_dt.replace(hour=23, minute=59, second=59)
    
    async for position_record in yield_position_records(db, position_class.collection):
        last_updated_dt = convert_timestamp_to_datetime(position_record['lastUpdatedTimestamp'])
        last_updated_dt = last_updated_dt.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=timezone.utc)

        missing_block, records_to_be_inserted = await process_position_for_lp_leaderboard(
            db, current_dt, last_updated_dt, position_record)
        
        if not missing_block:
            for record in records_to_be_inserted:
                await insert_lp_leaderboard_snapshot(record['event_data'], db, position_record=record['position_record'],
                                                     teahouse=position_class.teahouse)
            processed_positions_records += 1

    logger.info(f'Successfully processed {processed_positions_records} {position_class.log_msg}Positions records '
                f'for the leaderboard contest')


async def calculate_lp_leaderboard_user_total_points(db: Database, rpc_url: str, position_class: NftPosition):
    processed_lp_records = 0
    for record in db[Collection.LP_LEADERBOARD_SNAPSHOT].find(position_class.lp_snapshot_query, batch_size=10
                                                              ).sort('timestamp', ASCENDING):
        position_record_in_event = record['position']
        latest_position_record = await position_class.get_latest_position(db=db, record=record, rpc_url=rpc_url)

        current_fees_usd, token0_fees_current, token1_fees_current, token0_price, token1_price = await get_current_position_total_fees_usd(
            record, latest_position_record, record, db, rpc_url, position_class.get_uncollected_fees)
        if current_fees_usd < ZERO_DECIMAL:
            current_fees_usd = ZERO_DECIMAL
        
        last_time_vested_value, current_time_vested_value, period = await get_time_vested_value(
            record, position_record_in_event, latest_position_record)

        pool_boost = await get_pool_boost(latest_position_record['token0Address'], latest_position_record['token1Address'])

        points = current_fees_usd * last_time_vested_value * pool_boost * Decimal(1000)

        position_update_data = dict()
        position_update_data['$set'] = dict()
        position_update_data['$inc'] = dict()
        position_update_data['$set']['lastUnclaimedFeesToken0'] = Decimal128(token0_fees_current)
        position_update_data['$set']['lastUnclaimedFeesToken1'] = Decimal128(token1_fees_current)
        position_update_data['$set']['timeVestedValue'] = Decimal128(current_time_vested_value)
        position_update_data['$set']['lastUpdatedTimestamp'] = record['timestamp']
        position_update_data['$inc']['lpPoints'] = Decimal128(points)

        await position_class.update_position(db, latest_position_record, position_update_data)

        lp_contest_snapshot_data = dict()
        lp_contest_snapshot_data['$set'] = dict()
        lp_contest_snapshot_data['$set']['currentFeesUsd'] = Decimal128(current_fees_usd)
        lp_contest_snapshot_data['$set']['lastUnclaimedFeesToken0'] = latest_position_record['lastUnclaimedFeesToken0']
        lp_contest_snapshot_data['$set']['lastUnclaimedFeesToken1'] = latest_position_record['lastUnclaimedFeesToken1']
        lp_contest_snapshot_data['$set']['currentUnclaimedFeesToken0'] = Decimal128(token0_fees_current)
        lp_contest_snapshot_data['$set']['currentUnclaimedFeesToken1'] = Decimal128(token1_fees_current)
        lp_contest_snapshot_data['$set']['token0Price'] = Decimal128(token0_price)
        lp_contest_snapshot_data['$set']['token1Price'] = Decimal128(token1_price)
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

        lp_contest_query = {'userAddress': latest_position_record[position_class.lp_contest_user_key]}
        db[Collection.LP_LEADERBOARD].update_one(lp_contest_query, lp_contest_data, upsert=True)

        processed_lp_records += 1
        if processed_lp_records % 100 == 0:
            logger.info(f'Processed {processed_lp_records} records')

    logger.info(f'Successfully calculated {processed_lp_records} {position_class.log_msg}user records '
                f'for the lp leaderboard contest')


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
    
    match_query = {
        'processed': False,
    }
    for record in db[Collection.VOLUME_LEADERBOARD_SNAPSHOT].find(match_query, batch_size=10
                                                                  ).sort('timestamp', ASCENDING):
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
        await handle_positions_for_lp_leaderboard(db, JediSwapPosition)
        await calculate_lp_leaderboard_user_total_points(db, rpc_url, JediSwapPosition)
        await handle_positions_for_lp_leaderboard(db, TeahousePosition)
        await calculate_lp_leaderboard_user_total_points(db, rpc_url, TeahousePosition)
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
