from collections import defaultdict
import time

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from server.const import Collection
from server.transform.lp_contest_updates import calculate_lp_leaderboard_points
from server.query_utils import get_recent_block_number

from structlog import get_logger


logger = get_logger(__name__)


DAY_INTERVAL = 86400  # 1 day


async def yield_position_records(db: Database, total_points_calculation: bool = False) -> dict:
    if total_points_calculation:
        query = {}
    else:
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
    users_result = defaultdict(int)
    async for position_record in yield_position_records(db, total_points_calculation=True):
        lp_points = position_record['lp_points'].to_decimal()
        owner_address = position_record['ownerAddress']
        users_result[owner_address] += lp_points

    processed_positions_records = 0
    lp_contest_update_operations = []
    for user, points in users_result.items():
        lp_contest_update_data = {
            '$set': {
                'points': Decimal128(points),
            }
        }
        lp_contest_update_operations.append(
            UpdateOne(
                {'userAddress': user},
                lp_contest_update_data,
                upsert=True
            ))
        processed_positions_records += 1

    if lp_contest_update_operations:
        db[Collection.LP_LEADERBOARD].bulk_write(lp_contest_update_operations)
    logger.info(f'Successfully calculated {processed_positions_records} Positions records for the leaderboard contest')


async def process_leaderboard(mongo_url: str, mongo_database: Database, rpc_url: str):
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        await handle_lp_leaderboard(db, rpc_url)
        await calculate_lp_leaderboard_user_total_points(db)


async def run_leaderboard_transformer(mongo_url: str, mongo_database: Database, rpc_url: str):
    while True:
        await process_leaderboard(mongo_url, mongo_database, rpc_url)
        time.sleep(DAY_INTERVAL)
