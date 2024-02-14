import time

from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from server.const import Collection, ZERO_ADDRESS, DEFAULT_DECIMALS, TIME_INTERVAL
from server.query_utils import get_token_record

from structlog import get_logger

logger = get_logger(__name__)


async def yield_position_records(db: Database, collection: str) -> dict:
    query = {
        'ownerAddress': {'$ne': ZERO_ADDRESS},
        '$or': [
            {'processed': {'$exists': False}},
            {'processed': False}
        ]}
    # await filter_by_the_latest_value(query)
    for record in db[collection].find(query):
        yield record


async def handle_positions(db: Database, rpc_url: str):
    processed_positions_records = 0
    positions_update_operations = []
    async for position_record in yield_position_records(db, Collection.POSITIONS):
        token0 = await get_token_record(db, position_record['token0Address'], rpc_url)
        token1 = await get_token_record(db, position_record['token1Address'], rpc_url)
        position_update_data = {'$set': {
            'token0Decimals': token0['decimals'],
            'token1Decimals': token0['decimals'],
            'processed': True,
        }}

        positions_update_operations.append(
            UpdateOne(
                {'_id': position_record['_id']}, 
                position_update_data
            ))
        processed_positions_records += 1

    if positions_update_operations:
        db[Collection.POSITIONS].bulk_write(positions_update_operations)
    logger.info(f'Successfully processed {processed_positions_records} Positions records')


async def handle_positions_fees(db: Database, rpc_url: str):
    processed_positions_records = 0
    positions_update_operations = []
    async for position_fee_record in yield_position_records(db, Collection.POSITION_FEES):
        token0 = await get_token_record(db, position_fee_record['token0Address'], rpc_url)
        token1 = await get_token_record(db, position_fee_record['token1Address'], rpc_url)
        position_fee_update_data = {'$set': {
            'token0Decimals': token0['decimals'],
            'token1Decimals': token0['decimals'],
            'processed': True,
        }}

        positions_update_operations.append(
            UpdateOne(
                {'_id': position_fee_record['_id']}, 
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
        await handle_positions_fees(db, rpc_url)


async def run_positions_transformer(mongo_url: str, mongo_database: Database, rpc_url: str):
    while True:
        await process_positions(mongo_url, mongo_database, rpc_url)
        time.sleep(TIME_INTERVAL)
