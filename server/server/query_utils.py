from pymongo.database import Database
from typing import List, Union

from server.const import Collection, FACTORY_ADDRESS, ZERO_DECIMAL128, ZERO_ADDRESS

from starknet_py.contract import ContractFunction
from starknet_py.net.client_models import Call
from starknet_py.net.full_node_client import FullNodeClient
from starknet_py.cairo.felt import decode_shortstring

from structlog import get_logger

logger = get_logger(__name__)


async def filter_by_the_latest_value(query: dict):
    query['_cursor.to'] = None


async def get_factory_record(db: Database) -> dict:
    factory_collection = db[Collection.FACTORIES]
    existing_factory_record = factory_collection.find_one({'address': FACTORY_ADDRESS})
    if existing_factory_record is None:
        FACTORY_RECORD = {
            'address': FACTORY_ADDRESS,
            'txCount': 0,
            'totalValueLockedETH': ZERO_DECIMAL128,
            'totalValueLockedUSD': ZERO_DECIMAL128,
            'totalVolumeETH': ZERO_DECIMAL128,
            'totalVolumeUSD': ZERO_DECIMAL128,
            'untrackedVolumeUSD': ZERO_DECIMAL128,
            'totalFeesETH': ZERO_DECIMAL128,
            'totalFeesUSD': ZERO_DECIMAL128,
        }
        factory_collection.insert_one(FACTORY_RECORD)
        return FACTORY_RECORD
    return existing_factory_record

async def get_pool_record(db: Database, pool_address: str) -> dict:
    # consider adding a cache mechanism
    pools_collection = db[Collection.POOLS]
    query = {'poolAddress': pool_address}
    await filter_by_the_latest_value(query)
    return pools_collection.find_one(query)

async def get_token_record(db: Database, token_address: str, rpc_url: str) -> dict:
    logger.info("Getting token", token_address=token_address, rpc_url=rpc_url)
    tokens_collection = db[Collection.TOKENS]
    query = {'tokenAddress': token_address}
    existing_token_record = tokens_collection.find_one(query)
    if existing_token_record is None:
        TOKEN_RECORD = {
            'tokenAddress': token_address,
            'symbol': await get_token_symbol(token_address, rpc_url),
            'name': await get_token_name(token_address, rpc_url),
            'decimals': await get_token_decimals(token_address, rpc_url),
            'derivedETH': ZERO_DECIMAL128,
            'totalValueLocked': ZERO_DECIMAL128,
            'totalValueLockedUSD': ZERO_DECIMAL128,
        }
        existing_token_record = tokens_collection.find_one(query) ## Hacky way to counter parallel get_tokens
        if existing_token_record is None:
            tokens_collection.insert_one(TOKEN_RECORD)
            return TOKEN_RECORD
    return existing_token_record


async def get_tokens_from_pool(db: Database, existing_pool: dict, rpc_url: str) -> tuple[dict, dict]:
    # consider adding a cache mechanism
    token0_address = existing_pool['token0']
    token1_address = existing_pool['token1']
    
    token0 = await get_token_record(db, token0_address, rpc_url)
    token1 = await get_token_record(db, token1_address, rpc_url)
    return token0, token1


async def get_token_name(token_address: str, rpc_url: str) -> str:
    try:
        result = await simple_call(token_address, "name", [], rpc_url)
    except:
        try:
            result = await simple_call(token_address, "get_name", [], rpc_url)
        except:
            return 'NonToken'
    return decode_shortstring(result[0]).strip("\x00")
    
async def get_token_symbol(token_address: str, rpc_url: str) -> str:
    try:
        result = await simple_call(token_address, "symbol", [], rpc_url)
    except:
        try:
            result = await simple_call(token_address, "get_symbol", [], rpc_url)
        except:
            return 'NonToken'
    return decode_shortstring(result[0]).strip("\x00")
    
async def get_token_decimals(token_address: str, rpc_url: str) -> int:
    try:
        result = await simple_call(token_address, "decimals", [], rpc_url)
    except:
        try:
            result = await simple_call(token_address, "get_decimals", [], rpc_url)
        except:
<<<<<<< HEAD
            return 0
    return result[0]
    
async def simple_call(contract_address: str, method: str, calldata: List[int], rpc_url: str):
    rpc = FullNodeClient(node_url=rpc_url)
    selector = ContractFunction.get_selector(method)
    call = Call(int(contract_address, 16), selector, calldata)
    try:
        return await rpc.call_contract(call)
    except Exception as e:
        logger.info("rpc call did not succeed", error=str(e), contract_address=contract_address, method=method, calldata=calldata)  
        raise
=======
            try:
                result = await contract.functions["get_decimals"].call()
            except:
                return 0
        return result[0]

async def get_position_from_position_id(db: Database, position_id: int) -> dict:
    query = {
        'positionId': position_id,
        'ownerAddress': {'$ne': ZERO_ADDRESS},
    }
    await filter_by_the_latest_value(query)
    return db[Collection.POSITIONS].find_one(query)

async def get_tokens_from_position_db(db: Database, position_id: int) -> tuple[dict | None, dict | None]:
    token0, token1 = None, None
    position_record = await get_position_from_position_id(db, position_id)

    query = {
        'tokenAddress': {
            '$in': [
                position_record['token0Address'],
                position_record['token1Address'],
            ]
        }
    }
    for record in db[Collection.TOKENS].find(query):
        if record['tokenAddress'] == position_record['token0Address']:
            token0 = record
        elif record['tokenAddress'] == position_record['token1Address']:
            token1 = record

    return token0, token1
>>>>>>> apply review comments
