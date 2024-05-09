from datetime import datetime, timedelta
from decimal import Decimal
import time

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from server.const import Collection, ZERO_DECIMAL128, ZERO_DECIMAL
from server.transform.lp_contest_updates import calculate_lp_leaderboard_points
from server.query_utils import get_recent_block_number

from structlog import get_logger


logger = get_logger(__name__)


DAY_INTERVAL = 86400  # 1 day
EARLY_ADOPTER_MULTIPLIER_CONST = 3
LAST_SWAPS_PERIOD_IN_HOURS = 24
SWAP_PERCENTILE_TRESHOLD = 0.75


async def yield_position_records(db: Database) -> dict:
    query = {
        'liquidity': {'$ne': 0},
    }
    for record in db[Collection.POSITIONS].find(query):
        yield record


async def handle_lp_leaderboard(db: Database, rpc_url: str):
    processed_positions_records = 0
    current_timestamp = int(time.time() * 1000)
    block_number = await get_recent_block_number(rpc_url)
    
    async for position_record in yield_position_records(db):
        event_data = {
            'positionId': position_record['positionId'],
            'timestamp': current_timestamp,
            'block': block_number,
        }
        await calculate_lp_leaderboard_points(event_data, db, rpc_url)
        processed_positions_records += 1

    logger.info(f'Successfully processed {processed_positions_records} Positions records for the leaderboard contest')


async def calculate_lp_leaderboard_user_total_points(db: Database):
    pipeline = [
        {
            '$match': {
                'lp_points': {'$exists': True},
                'liquidity': {'$ne': 0}
            }
        },
        {
            '$group': {
                '_id': '$ownerAddress',
                'totalPoints': {'$sum': '$lp_points'}
            }
        },
        {
            '$project': {
                'userAddress': '$_id',
                'points': {'$toDecimal': '$totalPoints'}
            }
        }
    ]
    results = db[Collection.POSITIONS].aggregate(pipeline)
    
    lp_contest_update_operations = [
        UpdateOne(
            {'userAddress': result['userAddress']},
            {'$set': {
                'points': result['points'] if result['points'].to_decimal() > ZERO_DECIMAL else ZERO_DECIMAL128
            }},
            upsert=True
        ) for result in list(results)
    ]

    if lp_contest_update_operations:
        db[Collection.LP_LEADERBOARD].bulk_write(lp_contest_update_operations)
    logger.info(f'Successfully calculated {len(lp_contest_update_operations)} user records for the lp leaderboard contest')


async def insert_volume_leaderboard_snapshot(db: Database, fess_usd: Decimal, record: dict):
    volume_leaderboard_snapshot_record = {
        'userAddress': record['sender'],
        'swapFeesUsd': Decimal128(fess_usd),
        'sybilMultiplier': 0,
        'earlyMultiplier': EARLY_ADOPTER_MULTIPLIER_CONST,
        'timestamp': record['timestamp'],
        'volumePoints': ZERO_DECIMAL128,
        'processed': False,
    }
    db[Collection.VOLUME_LEADERBOARD_SNAPSHOT].insert_one(volume_leaderboard_snapshot_record)


async def yield_volume_snapshot_records(db: Database) -> dict:
    query = {
        'processed': False,
    }
    for record in db[Collection.VOLUME_LEADERBOARD_SNAPSHOT].find(query):
        yield record


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
    
    async for record in yield_volume_snapshot_records(db):
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
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        await handle_lp_leaderboard(db, rpc_url)
        await calculate_lp_leaderboard_user_total_points(db)
        await calculate_volume_leaderboard_user_total_points(db)


async def run_leaderboard_transformer(mongo_url: str, mongo_database: Database, rpc_url: str):
    while True:
        await process_leaderboard(mongo_url, mongo_database, rpc_url)
        time.sleep(DAY_INTERVAL)
