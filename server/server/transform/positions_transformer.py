# TODO: remove file and implement this logic in the indexer
import time

from pymongo import MongoClient, UpdateOne
from pymongo.database import Database
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient

from server.const import Collection, NFT_ROUTER, ZERO_ADDRESS, DEFAULT_DECIMALS, TIME_INTERVAL
from server.query_utils import filter_by_the_latest_value, get_token

from structlog import get_logger

logger = get_logger(__name__)


async def get_tokens_decimals_from_position(db: Database, rpc_url: str, position_id: int) -> tuple[int, int]:
    contract = await Contract.from_address(
        address=NFT_ROUTER,
        provider=FullNodeClient(node_url=rpc_url),
    )
    if contract is not None:
        (result, ) = await contract.functions['get_position'].call(position_id)
        token0_address = hex(result[1]['token0'])
        token1_address = hex(result[1]['token1'])

        token0 = await get_token(db, token0_address, rpc_url)
        token1 = await get_token(db, token1_address, rpc_url)
        return token0['decimals'], token1['decimals']
    
    return DEFAULT_DECIMALS, DEFAULT_DECIMALS


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
        positions_update_operations.append(
            UpdateOne({"_id": record['_id']}, {
                "$set": {
                    "token0Decimals": token0_decimals,
                    "token1Decimals": token1_decimals,
                    "processed": True,
                    }}))
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
