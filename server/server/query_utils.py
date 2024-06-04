from pymongo.database import Database
from typing import List, Optional, Any

from server.const import Collection, FACTORY_ADDRESS, ZERO_DECIMAL128
from server.utils import amount_after_decimals, get_hour_id

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

async def get_token_record(db: Database, token_address: str, rpc_url: Optional[str] = None) -> dict:
    # logger.info("Getting token", token_address=token_address, rpc_url=rpc_url)
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

async def get_token_hour_record(db: Database, token_address: str, hourId: int) -> dict:
    # consider adding a cache mechanism
    query = {'tokenAddress': token_address, 'hourId': hourId}
    return db[Collection.TOKENS_HOUR_DATA].find_one(query)


async def get_transaction_value_data(db, key) -> dict:
    pool_record = await get_pool_record(db, key[0])
    token0_record = await get_token_record(db, pool_record['token0'])
    token1_record = await get_token_record(db, pool_record['token1'])
    hour_id, _ = await get_hour_id(key[1])
    token0_hour_data = await get_token_hour_record(db, token0_record['tokenAddress'], hour_id)
    token1_hour_data = await get_token_hour_record(db, token1_record['tokenAddress'], hour_id)
    price0USD = token0_hour_data['close'].to_decimal()
    price1USD = token1_hour_data['close'].to_decimal()
    amount0 = await amount_after_decimals(abs(key[3]), token0_record['decimals'])
    amount1 = await amount_after_decimals(abs(key[4]), token1_record['decimals'])
    if (key[2] == 'Swap'):
        if (key[3] > 0):
            tx_value_usd = amount0 * price0USD
        else:
            tx_value_usd = amount1 * price1USD
    else:
        tx_value_usd = amount0 * price0USD + amount1 * price1USD
    return {"price0USD": str(price0USD), "price1USD": str(price1USD), "txValueUSD": str(tx_value_usd)}


async def get_tokens_from_pool(db: Database, existing_pool: dict, rpc_url: str) -> tuple[dict, dict]:
    # consider adding a cache mechanism
    token0_address = existing_pool['token0']
    token1_address = existing_pool['token1']
    
    token0 = await get_token_record(db, token0_address, rpc_url)
    token1 = await get_token_record(db, token1_address, rpc_url)
    return token0, token1


async def get_all_token_pools(db: Database, token_address: str) -> list[dict]:
    query = {
        '$or': [
            {'token0': token_address},
            {'token1': token_address},
        ]
    }
    return db[Collection.POOLS].find(query)

async def get_position_record(db: Database, position_id: str) -> dict:
    position_collection = db[Collection.POSITIONS]
    position_record = db[Collection.POSITIONS].find_one({'positionId': position_id})
    if position_record is None:
        position_record = {
            'positionId': position_id,
            'poolFee': 0,
            'tickLower': 0,
            'tickUpper': 0,
            'liquidity': 0,
            'depositedToken0': ZERO_DECIMAL128,
            'depositedToken1': ZERO_DECIMAL128,
            'withdrawnToken0': ZERO_DECIMAL128,
            'withdrawnToken1': ZERO_DECIMAL128,
            'collectedFeesToken0': ZERO_DECIMAL128,
            'collectedFeesToken1': ZERO_DECIMAL128,
            'totalFeesUSD': ZERO_DECIMAL128,
            'timeVestedValue': ZERO_DECIMAL128,
            'lastUpdatedTimestamp': 0,
            'lpPoints': ZERO_DECIMAL128,
        }
        position_collection.insert_one(position_record)
    return position_record


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

async def simulate_tx(tx: Any, rpc_url: str, block_number: int):
    rpc = FullNodeClient(node_url=rpc_url)
    simulated_txs = await rpc.simulate_transactions(
        transactions=[tx], skip_validate=True, skip_fee_charge=True, block_number=block_number)
    return simulated_txs[0].transaction_trace.execute_invocation.result

async def get_recent_block_number(rpc_url: str) -> int:
    rpc = FullNodeClient(node_url=rpc_url)
    try:
        return await rpc.get_block_number()
    except Exception as e:
        logger.info("rpc call did not succeed", error=str(e))  
        raise
