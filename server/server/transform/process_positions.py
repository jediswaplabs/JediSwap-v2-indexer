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


def get_tokens_decimals_from_position(db: Database, rpc_url: str, position_id: int) -> tuple[int, int]:
    # TODO: consider adding async call
    contract = Contract.from_address_sync(
        address=NFT_ROUTER,
        provider=FullNodeClient(node_url=rpc_url),
    )
    if contract is not None:
        (result, ) = contract.functions['get_position'].call_sync(position_id)
        token0_address = hex(result[1]['token0'])
        token1_address = hex(result[1]['token1'])

        token0 = get_token(db, token0_address, rpc_url)
        token1 = get_token(db, token1_address, rpc_url)
        return token0['decimals'], token1['decimals']
    
    return DEFAULT_DECIMALS, DEFAULT_DECIMALS


def yield_position_records(db: Database, collection: str) -> dict:
    query = {
        'ownerAddress': {'$ne': ZERO_ADDRESS},
        '$or': [
            {'processed': {'$exists': False}},
            {'processed': False}
        ]}
    filter_by_the_latest_value(query)
    for record in db[collection].find(query):
        yield record


def handle_positions(db: Database, rpc_url: str):
    processed_positions_records = 0
    positions_update_operations = []
    for record in yield_position_records(db, Collection.POSITIONS):
        token0_decimals , token1_decimals = get_tokens_decimals_from_position(db, rpc_url, record['positionId'])
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


def handle_positions_fees(db: Database):
    processed_positions_records = 0
    positions_update_operations = []
    for record in yield_position_records(db, Collection.POSITION_FEES):
        query = {
            'positionId': record['positionId'],
            'ownerAddress': {'$ne': ZERO_ADDRESS},
            }
        filter_by_the_latest_value(query)
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


def process_positions(mongo_url: str, mongo_database: Database, rpc_url: str):
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        handle_positions(db, rpc_url)
        handle_positions_fees(db)


def run(mongo_url: str, mongo_database: Database, rpc_url: str):
    while True:
        process_positions(mongo_url, mongo_database, rpc_url)
        time.sleep(TIME_INTERVAL)
