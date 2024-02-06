import time
from decimal import Decimal

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient

from server.const import Collection, FACTORY_ADDRESS, ZERO_DECIMAL128, TIME_INTERVAL
from server.interval_updates import (
    update_factory_day_data,
    update_pool_day_data,
    update_pool_hour_data,
    update_token_day_data,
    update_token_hour_data
)
from server.pricing import EthPrice, find_eth_per_token, sqrt_price_x96_to_token_prices, get_tracked_amount_usd
from server.query_utils import get_pool, get_tokens_from_pool, filter_by_the_latest_value
from server.utils import to_decimal, convert_num_to_decimal128

from pymongo import MongoClient, UpdateOne
from structlog import get_logger

logger = get_logger(__name__)


class Event:
    INITIALIZE = 'Initialize'
    MINT = 'Mint'
    BURN = 'Burn'
    SWAP = 'Swap'
    COLLECT = 'Collect'


class EventTracker:
    initialize_count = 0
    mint_count = 0
    swap_count = 0
    burn_count = 0


def update_tokens_records(db: Database, token0_id: str, token1_id: str, token0_update_data: dict, token1_update_data: dict):
    tokens_update_operations = [
        UpdateOne({"_id": token0_id}, token0_update_data),
        UpdateOne({"_id": token1_id}, token1_update_data),
    ]
    db[Collection.TOKENS].bulk_write(tokens_update_operations)


def update_pool_record(db: Database, pool_id: str, pool_update_data: dict):
    pool_query = {"_id": pool_id}
    filter_by_the_latest_value(pool_query)
    db[Collection.POOLS].update_one(pool_query, pool_update_data)


def update_factory_record(db: Database, factory_update_data: dict):
    factory_collection = db[Collection.FACTORIES]
    factory_collection.update_one({'address': FACTORY_ADDRESS}, factory_update_data)


def yield_pool_data_records(db: Database) -> dict:
    # TODO: get records from a specific pool
    last_block_record = db[Collection.POOLS_DATA].find(
        {'processed': True}, { 'block': 1, '_id': 0}
        ).sort({'block': -1}).limit(1)
    try:
        last_block = next(last_block_record)['block']
    except StopIteration:
        last_block = 0

    records_query = {
        'block': { '$gt': last_block},
        '$or': [
            {'processed': {'$exists': False}},
            {'processed': False}
        ]}
    for record in db[Collection.POOLS_DATA].find(records_query).sort('timestamp', 1):

        yield record


def get_factory_record(db: Database) -> dict:
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


def handle_initialize(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']

    del record['event']
    logger.info("handle Initialize", **record)

    pool = get_pool(db, record['poolAddress'])
    token0, token1 = get_tokens_from_pool(db, pool, rpc_url)

    pool_update_data = {
        '$set': {
            'sqrtPriceX96': record['sqrtPriceX96'],
            'tick': record['tick'],
            'totalValueLockedETH': ZERO_DECIMAL128,
            'totalValueLockedUSD': ZERO_DECIMAL128,
            'totalValueLockedToken0': ZERO_DECIMAL128,
            'totalValueLockedToken1': ZERO_DECIMAL128,
            'liquidity': ZERO_DECIMAL128,
            'token0Price': ZERO_DECIMAL128,
            'token1Price': ZERO_DECIMAL128,
            'feeGrowthGlobal0X128': '0x0',
            'feeGrowthGlobal1X128': '0x0',
            'txCount': 0,
        }
    }

    update_pool_record(db, pool['_id'], pool_update_data)

    EthPrice.set(rpc_url)

    pool = get_pool(db, record['poolAddress'])
    update_pool_day_data(db, pool, record['timestamp'])
    update_pool_hour_data(db, pool, record['timestamp'])

    token0_update_data = {
        '$set': {
            'derivedETH': Decimal128(find_eth_per_token(db, token0['tokenAddress'], rpc_url)),
        }
    }
    token1_update_data = {
        '$set': {
            'derivedETH': Decimal128(find_eth_per_token(db, token1['tokenAddress'], rpc_url)),
        }
    }
    update_tokens_records(db, token0['_id'], token1['_id'], token0_update_data, token1_update_data)

    EventTracker.initialize_count += 1


def handle_mint(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']
    factory = get_factory_record(db)

    del record['event']
    logger.info("handle Mint", **record)

    pool = get_pool(db, record['poolAddress'])
    token0, token1 = get_tokens_from_pool(db, pool, rpc_url)
    amount0 = to_decimal(record['amount0'], token0['decimals'])
    amount1 = to_decimal(record['amount1'], token1['decimals'])

    token0_derivedETH = token0['derivedETH'].to_decimal()
    token1_derivedETH = token1['derivedETH'].to_decimal()

    token0_update_data = dict()
    token0_update_data['$inc'] = dict()
    token0_update_data['$set'] = dict()
    token0_update_data['$set']['totalValueLockedUSD'] = Decimal128((token0['totalValueLocked'].to_decimal() + amount0) 
                                                                   * token0_derivedETH * EthPrice.get())
    token0_update_data['$inc']['totalValueLocked'] = Decimal128(amount0)
    token0_update_data['$inc']['txCount'] = 1

    token1_update_data = dict()
    token1_update_data['$inc'] = dict()
    token1_update_data['$set'] = dict()
    token1_update_data['$set']['totalValueLockedUSD'] = Decimal128((token1['totalValueLocked'].to_decimal() + amount1) 
                                                                   * token1_derivedETH * EthPrice.get())
    token1_update_data['$inc']['totalValueLocked'] = Decimal128(amount1)
    token1_update_data['$inc']['txCount'] = 1

    pool_totalValueLockedETH = pool['totalValueLockedETH'].to_decimal()
    factory_totalValueLockedETH = factory['totalValueLockedETH'].to_decimal() - pool_totalValueLockedETH

    pool_liquidity = Decimal(0)
    pool_tick = pool.get('tick')
    if pool_tick is not None and record['tickLower'] <= pool_tick < record['tickUpper']:
        pool_liquidity = Decimal(record['amount'])

    pool_totalValueLockedToken0 = pool['totalValueLockedToken0'].to_decimal()
    pool_totalValueLockedToken1 = pool['totalValueLockedToken1'].to_decimal()
    pool_totalValueLockedETH = ((pool_totalValueLockedToken0 + amount0) * token0_derivedETH) + (
        (pool_totalValueLockedToken1 + amount1) * token1_derivedETH)
    
    pool_update_data = dict()
    pool_update_data['$inc'] = dict()
    pool_update_data['$set'] = dict()
    pool_update_data['$set']['totalValueLockedETH'] = Decimal128(pool_totalValueLockedETH)
    pool_update_data['$set']['totalValueLockedUSD'] = Decimal128(pool_totalValueLockedETH * EthPrice.get())
    pool_update_data["$inc"]["liquidity"] = Decimal128(pool_liquidity)
    pool_update_data["$inc"]['totalValueLockedToken0'] = Decimal128(amount0)
    pool_update_data["$inc"]['totalValueLockedToken1'] = Decimal128(amount1)
    pool_update_data['$inc']['txCount'] = 1

    factory_update_data = dict()
    factory_update_data['$inc'] = dict()
    factory_update_data['$set'] = dict()
    factory_update_data['$set']['totalValueLockedUSD'] = Decimal128((factory_totalValueLockedETH + pool_totalValueLockedETH) 
                                                                    * EthPrice.get())
    factory_update_data['$set']['totalValueLockedETH'] = Decimal128(factory_totalValueLockedETH + pool_totalValueLockedETH)
    factory_update_data['$inc']['txCount'] = 1

    update_tokens_records(db, token0['_id'], token1['_id'], token0_update_data, token1_update_data)
    update_pool_record(db, pool['_id'], pool_update_data)
    update_factory_record(db, factory_update_data)
    
    update_factory_day_data(db, factory, record['timestamp'])
    update_pool_day_data(db, pool, record['timestamp'])
    update_pool_hour_data(db, pool, record['timestamp'])
    update_token_day_data(db, token0, record['timestamp'])
    update_token_hour_data(db, token0, record['timestamp'])
    update_token_day_data(db, token1, record['timestamp'])
    update_token_hour_data(db, token1, record['timestamp'])

    EventTracker.mint_count += 1


def handle_burn(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']
    factory = get_factory_record(db)

    del record['event']
    logger.info("handle Burn", **record)

    pool = get_pool(db, record['poolAddress'])
    token0, token1 = get_tokens_from_pool(db, pool, rpc_url)
    amount0 = to_decimal(record['amount0'], token0['decimals'])
    amount1 = to_decimal(record['amount1'], token1['decimals'])

    token0_derivedETH = token0['derivedETH'].to_decimal()
    token1_derivedETH = token1['derivedETH'].to_decimal()

    token0_update_data = dict()
    token0_update_data['$inc'] = dict()
    token0_update_data['$set'] = dict()
    token0_update_data['$set']['totalValueLockedUSD'] = Decimal128((token0['totalValueLocked'].to_decimal() - amount0) 
                                                                   * token0_derivedETH * EthPrice.get())
    token0_update_data['$inc']['totalValueLocked'] = Decimal128(-amount0)
    token0_update_data['$inc']['txCount'] = 1

    token1_update_data = dict()
    token1_update_data['$inc'] = dict()
    token1_update_data['$set'] = dict()
    token1_update_data['$set']['totalValueLockedUSD'] = Decimal128((token1['totalValueLocked'].to_decimal() - amount1) 
                                                                   * token1_derivedETH * EthPrice.get())
    token1_update_data['$inc']['totalValueLocked'] = Decimal128(-amount1)
    token1_update_data['$inc']['txCount'] = 1

    pool_totalValueLockedETH = pool['totalValueLockedETH'].to_decimal()
    factory_totalValueLockedETH = factory['totalValueLockedETH'].to_decimal() - pool_totalValueLockedETH

    pool_liquidity = Decimal(0)
    pool_tick = pool.get('tick')
    if pool_tick is not None and record['tickLower'] <= pool_tick < record['tickUpper']:
        pool_liquidity = Decimal(-record['amount'])

    pool_totalValueLockedToken0 = pool['totalValueLockedToken0'].to_decimal()
    pool_totalValueLockedToken1 = pool['totalValueLockedToken1'].to_decimal()
    pool_totalValueLockedETH = ((pool_totalValueLockedToken0 - amount0) * token0_derivedETH) + (
        (pool_totalValueLockedToken1 - amount1) * token1_derivedETH)

    pool_update_data = dict()
    pool_update_data['$inc'] = dict()
    pool_update_data['$set'] = dict()
    pool_update_data['$set']['totalValueLockedETH'] = Decimal128(pool_totalValueLockedETH)
    pool_update_data['$set']['totalValueLockedUSD'] = Decimal128(pool_totalValueLockedETH * EthPrice.get())
    pool_update_data["$inc"]["liquidity"] = Decimal128(pool_liquidity)
    pool_update_data["$inc"]['totalValueLockedToken0'] = Decimal128(-amount0)
    pool_update_data["$inc"]['totalValueLockedToken1'] = Decimal128(-amount1)
    pool_update_data['$inc']['txCount'] = 1

    factory_update_data = dict()
    factory_update_data['$inc'] = dict()
    factory_update_data['$set'] = dict()
    factory_update_data['$set']['totalValueLockedUSD'] = Decimal128((factory_totalValueLockedETH + pool_totalValueLockedETH) 
                                                                    * EthPrice.get())
    factory_update_data['$set']['totalValueLockedETH'] = Decimal128(factory_totalValueLockedETH + pool_totalValueLockedETH)
    factory_update_data['$inc']['txCount'] = 1

    update_tokens_records(db, token0['_id'], token1['_id'], token0_update_data, token1_update_data)
    update_pool_record(db, pool['_id'], pool_update_data)
    update_factory_record(db, factory_update_data)
    
    update_factory_day_data(db, factory, record['timestamp'])
    update_pool_day_data(db, pool, record['timestamp'])
    update_pool_hour_data(db, pool, record['timestamp'])
    update_token_day_data(db, token0, record['timestamp'])
    update_token_hour_data(db, token0, record['timestamp'])
    update_token_day_data(db, token1, record['timestamp'])
    update_token_hour_data(db, token1, record['timestamp'])

    EventTracker.burn_count += 1


def handle_swap(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    factory = get_factory_record(db)
    rpc_url = kwargs['rpc_url']

    del record['event']
    logger.info("handle Swap", **record)

    pool = get_pool(db, record['poolAddress'])
    token0, token1 = get_tokens_from_pool(db, pool, rpc_url)
    amount0 = to_decimal(record['amount0'], token0['decimals'])
    amount1 = to_decimal(record['amount1'], token1['decimals'])

    # TODO
    old_tick = pool.get('tick')

    token0_derivedETH = token0['derivedETH'].to_decimal()
    token1_derivedETH = token1['derivedETH'].to_decimal()

    amount0_abs = abs(amount0)
    amount1_abs = abs(amount1)

    amount0_ETH = amount0_abs * token0_derivedETH
    amount1_ETH = amount1_abs * token1_derivedETH

    amount0_USD = amount0_ETH * EthPrice.get()
    amount1_USD = amount1_ETH * EthPrice.get()

    amount_total_USD_tracked = get_tracked_amount_usd(amount0_abs, token0['tokenAddress'], token0_derivedETH, amount1_abs, token1['tokenAddress'], token1_derivedETH) / 2
    amount_total_ETH_tracked = amount_total_USD_tracked / EthPrice.get()

    amount_total_USD_untracked = (amount0_USD + amount1_USD) / 2

    fees_ETH = amount_total_ETH_tracked * pool['fee'] / 1000000
    fees_USD = amount_total_USD_untracked * pool['fee'] / 1000000

    factory_update_data = dict()
    factory_update_data['$inc'] = dict()
    factory_update_data['$set'] = dict()
    factory_update_data['$inc']['txCount'] = 1
    factory_update_data['$inc']['totalVolumeETH'] = Decimal128(amount_total_ETH_tracked)
    factory_update_data['$inc']['totalVolumeUSD'] = Decimal128(amount_total_USD_tracked)
    factory_update_data['$inc']['untrackedVolumeUSD'] = Decimal128(amount_total_USD_untracked)
    factory_update_data['$inc']['totalFeesETH'] = Decimal128(fees_ETH)
    factory_update_data['$inc']['totalFeesUSD'] = Decimal128(fees_USD)

    pool_update_data = dict()
    pool_update_data['$inc'] = dict()
    pool_update_data['$set'] = dict()
    pool_update_data['$inc']['volumeToken0'] = Decimal128(amount0_abs)
    pool_update_data['$inc']['volumeToken1'] = Decimal128(amount1_abs)
    pool_update_data['$inc']['volumeUSD'] = Decimal128(amount_total_USD_tracked)
    pool_update_data['$inc']['untrackedVolumeUSD'] = Decimal128(amount_total_USD_untracked)
    pool_update_data['$inc']['feesUSD'] = Decimal128(fees_USD)
    pool_update_data['$inc']['txCount'] = 1
    
    pool_update_data['$set']['liquidity'] = convert_num_to_decimal128(record['liquidity'])
    pool_update_data['$set']['tick'] = record['tick']
    pool_update_data['$set']['sqrtPriceX96'] = record['sqrtPriceX96']
    pool_totalValueLockedToken0 = pool['totalValueLockedToken0'].to_decimal() + amount0
    pool_totalValueLockedToken1 = pool['totalValueLockedToken1'].to_decimal() + amount1
    pool_update_data['$set']['totalValueLockedToken0'] = Decimal128(pool_totalValueLockedToken0)
    pool_update_data['$set']['totalValueLockedToken1'] = Decimal128(pool_totalValueLockedToken1)

    token0_update_data = dict()
    token0_update_data['$inc'] = dict()
    token0_update_data['$set'] = dict()
    token0_update_data['$inc']['volume'] = Decimal128(amount0_abs)
    token0_totalValueLocked = token0['totalValueLocked'].to_decimal() + amount0
    token0_update_data['$inc']['volumeUSD'] = Decimal128(amount_total_USD_tracked)
    token0_update_data['$inc']['untrackedVolumeUSD'] = Decimal128(amount_total_USD_untracked)
    token0_update_data['$inc']['feesUSD'] = Decimal128(fees_USD)
    token0_update_data['$inc']['txCount'] = 1

    token1_update_data = dict()
    token1_update_data['$inc'] = dict()
    token1_update_data['$set'] = dict()
    token1_update_data['$inc']['volume'] = Decimal128(amount1_abs)
    token1_totalValueLocked = token1['totalValueLocked'].to_decimal() + amount1
    token1_update_data['$inc']['volumeUSD'] = Decimal128(amount_total_USD_tracked)
    token1_update_data['$inc']['untrackedVolumeUSD'] = Decimal128(amount_total_USD_untracked)
    token1_update_data['$inc']['feesUSD'] = Decimal128(fees_USD)
    token1_update_data['$inc']['txCount'] = 1

    prices = sqrt_price_x96_to_token_prices(record['sqrtPriceX96'], token0['decimals'], token1['decimals'])
    pool_update_data['$set']['token0Price'] = Decimal128(prices[0])
    pool_update_data['$set']['token1Price'] = Decimal128(prices[1])

    EthPrice.set(rpc_url)

    token0_derivedETH = find_eth_per_token(db, token0, rpc_url)
    token1_derivedETH = find_eth_per_token(db, token1, rpc_url)
    token0_update_data['$set']['derivedETH'] = Decimal128(token0_derivedETH)
    token1_update_data['$set']['derivedETH'] = Decimal128(token1_derivedETH)
    
    factory_totalValueLockedETH = factory['totalValueLockedETH'].to_decimal() - pool['totalValueLockedETH'].to_decimal()

    pool_totalValueLockedETH = (pool_totalValueLockedToken0 * token0_derivedETH) + (pool_totalValueLockedToken1 * token1_derivedETH)
    pool_update_data['$set']['totalValueLockedETH'] = Decimal128(pool_totalValueLockedETH)
    pool_update_data['$set']['totalValueLockedUSD'] = Decimal128(pool_totalValueLockedETH * EthPrice.get())

    factory_totalValueLockedETH = factory_totalValueLockedETH + pool_totalValueLockedETH
    factory_update_data['$set']['totalValueLockedETH'] = Decimal128(factory_totalValueLockedETH)
    factory_update_data['$set']['totalValueLockedUSD'] = Decimal128(factory_totalValueLockedETH * EthPrice.get())

    token1_update_data['$set']['totalValueLocked'] = Decimal128(token0_totalValueLocked)
    token1_update_data['$set']['totalValueLockedUSD'] = Decimal128(token0_totalValueLocked * token0_derivedETH * EthPrice.get())

    token1_update_data['$set']['totalValueLocked'] = Decimal128(token1_totalValueLocked)
    token1_update_data['$set']['totalValueLockedUSD'] = Decimal128(token1_totalValueLocked * token1_derivedETH * EthPrice.get())

    # TODO Update fee growth
    contract = Contract.from_address_sync(address=record['poolAddress'], provider=FullNodeClient(node_url=rpc_url))
    if contract is not None:
        (fee_growth_global_0_X128,) = contract.functions["get_fee_growth_global_0_X128"].call_sync()
        pool_update_data['$set']['feeGrowthGlobal0X128'] = hex(fee_growth_global_0_X128)
        (fee_growth_global_1_X128,) = contract.functions["get_fee_growth_global_1_X128"].call_sync()
        pool_update_data['$set']['feeGrowthGlobal1X128'] = hex(fee_growth_global_1_X128)

    update_tokens_records(db, token0['_id'], token1['_id'], token0_update_data, token1_update_data)
    update_pool_record(db, pool['_id'], pool_update_data)
    update_factory_record(db, factory_update_data)
    
    update_factory_day_data(db, factory, record['timestamp'], amount_total_ETH_tracked, amount_total_USD_tracked, fees_USD)
    update_pool_day_data(db, pool, record['timestamp'], amount_total_USD_tracked, amount0_abs, amount1_abs, fees_USD)
    update_pool_hour_data(db, pool, record['timestamp'], amount_total_USD_tracked, amount0_abs, amount1_abs, fees_USD)
    update_token_day_data(db, token0, record['timestamp'], amount_total_USD_tracked, amount0_abs, fees_USD)
    update_token_hour_data(db, token0, record['timestamp'], amount_total_USD_tracked, amount0_abs, fees_USD)
    update_token_day_data(db, token1, record['timestamp'], amount_total_USD_tracked, amount1_abs, fees_USD)
    update_token_hour_data(db, token1, record['timestamp'], amount_total_USD_tracked, amount1_abs, fees_USD)
    
    EventTracker.swap_count += 1


EVENT_TO_FUNCTION_MAP = {
    Event.INITIALIZE: handle_initialize,
    Event.MINT: handle_mint,
    Event.SWAP: handle_swap,
    Event.BURN: handle_burn,
}


def process_events(mongo_url: str, mongo_database: Database, rpc_url: str):
    processed_records = []
    EthPrice.set(rpc_url)
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        for record in yield_pool_data_records(db):
            event_func = EVENT_TO_FUNCTION_MAP.get(record['event'])
            if event_func:
                event_func(
                    db=db, 
                    record=record,
                    rpc_url=rpc_url)
                processed_records.append(
                    UpdateOne({"_id": record['_id']}, {"$set": {"processed": True}})
                )
        if processed_records:
            db[Collection.POOLS_DATA].bulk_write(processed_records)

    logger.info(f'Successfully processed {EventTracker.initialize_count} Initialize events')
    logger.info(f'Successfully processed {EventTracker.mint_count} Mint events')
    logger.info(f'Successfully processed {EventTracker.swap_count} Swap events')
    logger.info(f'Successfully processed {EventTracker.burn_count} Burn events')
    EventTracker.initialize_count = 0
    EventTracker.mint_count = 0
    EventTracker.swap_count = 0

    EventTracker.burn_count = 0


def run(mongo_url: str, mongo_database: Database, rpc_url: str):
    while True:
        process_events(mongo_url, mongo_database, rpc_url)
        time.sleep(TIME_INTERVAL)
