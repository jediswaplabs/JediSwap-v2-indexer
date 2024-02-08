import time

from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from server.const import Collection, DEFAULT_DECIMALS, ZERO_ADDRESS, TIME_INTERVAL
from server.query_utils import filter_by_the_latest_value, get_token_record, get_position_from_position_id, get_tokens_from_position_db

from structlog import get_logger

logger = get_logger(__name__)


async def get_tokens_decimals_from_position(db: Database, rpc_url: str, position_id: int) -> tuple[int, int]:
    position_record = await get_position_from_position_id(db, position_id)
    if not position_record:
        return DEFAULT_DECIMALS, DEFAULT_DECIMALS

    token0 = await get_token_record(db, position_record['token0Address'], rpc_url)
    token1 = await get_token_record(db, position_record['token1Address'], rpc_url)
    return token0['decimals'], token1['decimals']


async def yield_position_records(db: Database, collection: str) -> dict:
    query = {
        'ownerAddress': {'$ne': ZERO_ADDRESS},
        '$or': [
            {'processed': {'$exists': False}},
            {'processed': False}
        ]}
    await filter_by_the_latest_value(query)
    for record in db[collection].find(query):
        yield record


async def handle_positions(db: Database, rpc_url: str):
    processed_positions_records = 0
    positions_update_operations = []
    async for record in yield_position_records(db, Collection.POSITIONS):
        token0_decimals , token1_decimals = await get_tokens_decimals_from_position(db, rpc_url, record['positionId'])
        position_update_data = {'$set': {
            'token0Decimals': token0_decimals,
            'token1Decimals': token1_decimals,
            'processed': True,
        }}

        positions_update_operations.append(
            UpdateOne(
                {'_id': record['_id']}, 
                position_update_data
                ))
        processed_positions_records += 1

    if positions_update_operations:
        db[Collection.POSITIONS].bulk_write(positions_update_operations)
    logger.info(f'Successfully processed {processed_positions_records} Positions records')


async def handle_positions_fees(db: Database):
    processed_positions_records = 0
    positions_update_operations = []
    async for record in yield_position_records(db, Collection.POSITION_FEES):
        query = {
            'positionId': record['positionId'],
            'ownerAddress': {'$ne': ZERO_ADDRESS},
            }
        await filter_by_the_latest_value(query)
        position = db[Collection.POSITIONS].find_one(query)
        if position:
            positions_update_operations.append(
                UpdateOne({"_id": record['_id']}, {
                    "$set": {
                        "token0Decimals": position['token0Decimals'],
                        "token1Decimals": position['token1Decimals'],
                        "processed": True,
                        }}))
            processed_positions_records += 1

async def handle_positions_fees(db: Database, rpc_url: str):
    processed_positions_records = 0
    positions_update_operations = []
    async for record in yield_position_records(db, Collection.POSITION_FEES):
        token0_decimals , token1_decimals = get_tokens_decimals_from_position(db, rpc_url, record['positionId'])
        position_fee_update_data = {'$set': {
            'token0Decimals': token0_decimals,
            'token1Decimals': token1_decimals,
            'processed': True,
        }}

        positions_update_operations.append(
            UpdateOne(
                {'_id': record['_id']}, 
                position_fee_update_data
                ))
        processed_positions_records += 1

    if positions_update_operations:
        db[Collection.POSITION_FEES].bulk_write(positions_update_operations)
    logger.info(f'Successfully processed {processed_positions_records} Positions Feees records')


async def process_positions(mongo_url: str, mongo_database: Database, rpc_url: str):
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        await handle_positions(db, rpc_url)
        await handle_positions_fees(db)


async def run_positions_transformer(mongo_url: str, mongo_database: Database, rpc_url: str):
    while True:
        await process_positions(mongo_url, mongo_database, rpc_url)
        time.sleep(TIME_INTERVAL)
