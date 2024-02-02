from pymongo.database import Database

from server.const import Collection, ZERO_DECIMAL128

from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
from starknet_py.cairo.felt import decode_shortstring

from structlog import get_logger

logger = get_logger(__name__)


def filter_by_the_latest_value(query: dict):
    query['_cursor.to'] = None

def get_pool(db: Database, pool_address: str) -> dict:
    # consider adding a cache mechanism
    pools_collection = db[Collection.POOLS]
    query = {'poolAddress': pool_address}
    filter_by_the_latest_value(query)
    return pools_collection.find_one(query)

def get_token(db: Database, token_address: str, rpc_url: str) -> dict:
    # consider adding a cache mechanism
    logger.info("Getting token", token_address=token_address)
    tokens_collection = db[Collection.TOKENS]
    query = {'tokenAddress': token_address}
    existing_token_record = tokens_collection.find_one(query)
    if existing_token_record is None:
        TOKEN_RECORD = {
            'tokenAddress': token_address,
            'symbol': get_token_symbol(token_address, rpc_url),
            'name': get_token_name(token_address, rpc_url),
            'decimals': get_token_decimals(token_address, rpc_url),
            'derivedETH': ZERO_DECIMAL128,
            'totalValueLocked': ZERO_DECIMAL128,
            'totalValueLockedUSD': ZERO_DECIMAL128,
        }
        existing_token_record = tokens_collection.find_one(query) ## Hacky way to counter parallel get_tokens
        if existing_token_record is None:
            tokens_collection.insert_one(TOKEN_RECORD)
            return TOKEN_RECORD
    return existing_token_record


def get_tokens_from_pool(db: Database, existing_pool: dict, rpc_url: str) -> tuple[dict, dict]:
    # consider adding a cache mechanism
    token0_address = existing_pool['token0']
    token1_address = existing_pool['token1']
    
    token0 = get_token(db, token0_address, rpc_url)
    token1 = get_token(db, token1_address, rpc_url)
    return token0, token1


def get_token_name(token_address: str, rpc_url: str) -> str:
    contract = Contract.from_address_sync(address=token_address, provider=FullNodeClient(node_url=rpc_url))
    if contract is not None:
        try:
            result = contract.functions["name"].call_sync()
        except:
            try:
                result = contract.functions["get_name"].call_sync()
            except:
                return 'NonToken'
        return decode_shortstring(result[0]).strip("\x00")
    
def get_token_symbol(token_address: str, rpc_url: str) -> str:
    contract = Contract.from_address_sync(address=token_address, provider=FullNodeClient(node_url=rpc_url))
    if contract is not None:
        try:
            result = contract.functions["symbol"].call_sync()
        except:
            try:
                result = contract.functions["get_symbol"].call_sync()
            except:
                return 'NonToken'
        return decode_shortstring(result[0]).strip("\x00")
    
def get_token_decimals(token_address: str, rpc_url: str) -> int:
    contract = Contract.from_address_sync(address=token_address, provider=FullNodeClient(node_url=rpc_url))
    if contract is not None:
        try:
            result = contract.functions["decimals"].call_sync()
        except:
            try:
                result = contract.functions["get_decimals"].call_sync()
            except:
                return 0
        return result[0]
