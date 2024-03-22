import time
from decimal import Decimal

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient

from server.const import Collection, FACTORY_ADDRESS, ZERO_DECIMAL128, TIME_INTERVAL
from server.transform.interval_updates import (
    update_factory_day_data,
    update_factory_hour_data,
    update_pool_day_data,
    update_pool_hour_data,
    update_token_day_data,
    update_token_hour_data
)
from server.transform.pricing import EthPrice, find_eth_per_token, sqrt_price_x96_to_token_prices, get_tracked_amount_usd
from server.query_utils import get_factory_record, get_pool_record, get_token_record, get_tokens_from_pool, filter_by_the_latest_value
from server.utils import amount_after_decimals, convert_num_to_decimal128

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
    pool_addresses_to_update_fee_growth = set()


async def update_tokens_records(db: Database, token0_id: str, token1_id: str, token0_update_data: dict, token1_update_data: dict):
    tokens_update_operations = [
        UpdateOne({"_id": token0_id}, token0_update_data),
        UpdateOne({"_id": token1_id}, token1_update_data),
    ]
    db[Collection.TOKENS].bulk_write(tokens_update_operations)


async def update_pool_record(db: Database, pool_id: str, pool_update_data: dict):
    pool_query = {"_id": pool_id}
    await filter_by_the_latest_value(pool_query)
    db[Collection.POOLS].update_one(pool_query, pool_update_data)


async def update_factory_record(db: Database, factory_update_data: dict):
    factory_collection = db[Collection.FACTORIES]
    factory_collection.update_one({'address': FACTORY_ADDRESS}, factory_update_data)


async def yield_pool_data_records(db: Database) -> dict:
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

async def handle_initialize(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']

    del record['event']
    logger.info("handle Initialize", **record)

    pool = await get_pool_record(db, record['poolAddress'])
    token0, token1 = await get_tokens_from_pool(db, pool, rpc_url)

    prices = await sqrt_price_x96_to_token_prices(record['sqrtPriceX96'], token0['decimals'], token1['decimals'])

    pool_update_data = {
        '$set': {
            'sqrtPriceX96': record['sqrtPriceX96'],
            'tick': record['tick'],
            'totalValueLockedETH': ZERO_DECIMAL128,
            'totalValueLockedUSD': ZERO_DECIMAL128,
            'totalValueLockedToken0': ZERO_DECIMAL128,
            'totalValueLockedToken1': ZERO_DECIMAL128,
            'liquidity': ZERO_DECIMAL128,
            'token0Price': Decimal128(prices[0]),
            'token1Price': Decimal128(prices[1]),
            'feeGrowthGlobal0X128': '0x0',
            'feeGrowthGlobal1X128': '0x0',
            'txCount': 0,
        }
    }

    await update_pool_record(db, pool['_id'], pool_update_data)

    await EthPrice.set(db)

    pool = await get_pool_record(db, record['poolAddress'])

    await update_pool_day_data(db, pool, record['timestamp'])
    await update_pool_hour_data(db, pool, record['timestamp'])

    token0_update_data = {
        '$set': {
            'derivedETH': Decimal128(await find_eth_per_token(db, token0['tokenAddress'], rpc_url)),
        }
    }
    token1_update_data = {
        '$set': {
            'derivedETH': Decimal128(await find_eth_per_token(db, token1['tokenAddress'], rpc_url)),
        }
    }
    await update_tokens_records(db, token0['_id'], token1['_id'], token0_update_data, token1_update_data)

    EventTracker.initialize_count += 1


async def handle_mint(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']
    factory = await get_factory_record(db)

    del record['event']
    logger.info("handle Mint", **record)

    pool = await get_pool_record(db, record['poolAddress'])
    token0, token1 = await get_tokens_from_pool(db, pool, rpc_url)
    amount0 = await amount_after_decimals(record['amount0'], token0['decimals'])
    amount1 = await amount_after_decimals(record['amount1'], token1['decimals'])

    token0_derivedETH = token0['derivedETH'].to_decimal()
    token1_derivedETH = token1['derivedETH'].to_decimal()

    eth_price = await EthPrice.get()
    token0_update_data = dict()
    token0_update_data['$inc'] = dict()
    token0_update_data['$set'] = dict()
    token0_update_data['$set']['totalValueLockedUSD'] = Decimal128((token0['totalValueLocked'].to_decimal() + amount0) 
                                                                   * token0_derivedETH * eth_price)
    token0_update_data['$inc']['totalValueLocked'] = Decimal128(amount0)
    token0_update_data['$inc']['txCount'] = 1

    token1_update_data = dict()
    token1_update_data['$inc'] = dict()
    token1_update_data['$set'] = dict()
    token1_update_data['$set']['totalValueLockedUSD'] = Decimal128((token1['totalValueLocked'].to_decimal() + amount1) 
                                                                   * token1_derivedETH * eth_price)
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
    pool_update_data['$set']['totalValueLockedUSD'] = Decimal128(pool_totalValueLockedETH * eth_price)
    pool_update_data["$inc"]["liquidity"] = Decimal128(pool_liquidity)
    pool_update_data["$inc"]['totalValueLockedToken0'] = Decimal128(amount0)
    pool_update_data["$inc"]['totalValueLockedToken1'] = Decimal128(amount1)
    pool_update_data['$inc']['txCount'] = 1

    factory_update_data = dict()
    factory_update_data['$inc'] = dict()
    factory_update_data['$set'] = dict()
    factory_update_data['$set']['totalValueLockedUSD'] = Decimal128((factory_totalValueLockedETH + pool_totalValueLockedETH) 
                                                                    * eth_price)
    factory_update_data['$set']['totalValueLockedETH'] = Decimal128(factory_totalValueLockedETH + pool_totalValueLockedETH)
    factory_update_data['$inc']['txCount'] = 1

    await update_tokens_records(db, token0['_id'], token1['_id'], token0_update_data, token1_update_data)
    await update_pool_record(db, pool['_id'], pool_update_data)
    await update_factory_record(db, factory_update_data)

    factory = await get_factory_record(db)
    pool = await get_pool_record(db, record['poolAddress'])
    token0 = await get_token_record(db, token0['tokenAddress'], rpc_url)
    token1 = await get_token_record(db, token1['tokenAddress'], rpc_url)
    
    await update_factory_day_data(db, factory, record['timestamp'])
    await update_factory_hour_data(db, factory, record['timestamp'])
    await update_pool_day_data(db, pool, record['timestamp'])
    await update_pool_hour_data(db, pool, record['timestamp'])
    await update_token_day_data(db, token0, record['timestamp'])
    await update_token_hour_data(db, token0, record['timestamp'])
    await update_token_day_data(db, token1, record['timestamp'])
    await update_token_hour_data(db, token1, record['timestamp'])

    EventTracker.mint_count += 1


async def handle_burn(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']
    factory = await get_factory_record(db)

    del record['event']
    logger.info("handle Burn", **record)

    pool = await get_pool_record(db, record['poolAddress'])
    token0, token1 = await get_tokens_from_pool(db, pool, rpc_url)
    amount0 = await amount_after_decimals(record['amount0'], token0['decimals'])
    amount1 = await amount_after_decimals(record['amount1'], token1['decimals'])

    token0_derivedETH = token0['derivedETH'].to_decimal()
    token1_derivedETH = token1['derivedETH'].to_decimal()

    eth_price = await EthPrice.get()
    
    token0_update_data = dict()
    token0_update_data['$inc'] = dict()
    token0_update_data['$set'] = dict()
    token0_update_data['$set']['totalValueLockedUSD'] = Decimal128((token0['totalValueLocked'].to_decimal() - amount0) 
                                                                   * token0_derivedETH * eth_price)
    token0_update_data['$inc']['totalValueLocked'] = Decimal128(-amount0)
    token0_update_data['$inc']['txCount'] = 1

    token1_update_data = dict()
    token1_update_data['$inc'] = dict()
    token1_update_data['$set'] = dict()
    token1_update_data['$set']['totalValueLockedUSD'] = Decimal128((token1['totalValueLocked'].to_decimal() - amount1) 
                                                                   * token1_derivedETH * eth_price)
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
    pool_update_data['$set']['totalValueLockedUSD'] = Decimal128(pool_totalValueLockedETH * eth_price)
    pool_update_data["$inc"]["liquidity"] = Decimal128(pool_liquidity)
    pool_update_data["$inc"]['totalValueLockedToken0'] = Decimal128(-amount0)
    pool_update_data["$inc"]['totalValueLockedToken1'] = Decimal128(-amount1)
    pool_update_data['$inc']['txCount'] = 1

    factory_update_data = dict()
    factory_update_data['$inc'] = dict()
    factory_update_data['$set'] = dict()
    factory_update_data['$set']['totalValueLockedUSD'] = Decimal128((factory_totalValueLockedETH + pool_totalValueLockedETH) 
                                                                    * eth_price)
    factory_update_data['$set']['totalValueLockedETH'] = Decimal128(factory_totalValueLockedETH + pool_totalValueLockedETH)
    factory_update_data['$inc']['txCount'] = 1

    await update_tokens_records(db, token0['_id'], token1['_id'], token0_update_data, token1_update_data)
    await update_pool_record(db, pool['_id'], pool_update_data)
    await update_factory_record(db, factory_update_data)

    factory = await get_factory_record(db)
    pool = await get_pool_record(db, record['poolAddress'])
    token0 = await get_token_record(db, token0['tokenAddress'], rpc_url)
    token1 = await get_token_record(db, token1['tokenAddress'], rpc_url)
    
    await update_factory_day_data(db, factory, record['timestamp'])
    await update_factory_hour_data(db, factory, record['timestamp'])
    await update_pool_day_data(db, pool, record['timestamp'])
    await update_pool_hour_data(db, pool, record['timestamp'])
    await update_token_hour_data(db, token0, record['timestamp'])
    await update_token_day_data(db, token1, record['timestamp'])
    await update_token_hour_data(db, token1, record['timestamp'])

    EventTracker.burn_count += 1


async def handle_swap(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    factory = await get_factory_record(db)
    rpc_url = kwargs['rpc_url']

    del record['event']
    logger.info("handle Swap", **record)

    pool = await get_pool_record(db, record['poolAddress'])
    token0, token1 = await get_tokens_from_pool(db, pool, rpc_url)
    amount0 = await amount_after_decimals(record['amount0'], token0['decimals'])
    amount1 = await amount_after_decimals(record['amount1'], token1['decimals'])

    # TODO
    old_tick = pool.get('tick')

    token0_derivedETH = token0['derivedETH'].to_decimal()
    token1_derivedETH = token1['derivedETH'].to_decimal()

    amount0_abs = abs(amount0)
    amount1_abs = abs(amount1)

    amount0_ETH = amount0_abs * token0_derivedETH
    amount1_ETH = amount1_abs * token1_derivedETH

    eth_price = await EthPrice.get()
    amount0_USD = amount0_ETH * eth_price
    amount1_USD = amount1_ETH * eth_price

    amount_total_USD_tracked = await get_tracked_amount_usd(amount0_abs, token0['tokenAddress'], token0_derivedETH, amount1_abs, token1['tokenAddress'], token1_derivedETH) / 2
    amount_total_ETH_tracked = amount_total_USD_tracked / eth_price

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
    
    pool_update_data['$set']['liquidity'] = await convert_num_to_decimal128(record['liquidity'])
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

    prices = await sqrt_price_x96_to_token_prices(record['sqrtPriceX96'], token0['decimals'], token1['decimals'])
    pool_update_data['$set']['token0Price'] = Decimal128(prices[0])
    pool_update_data['$set']['token1Price'] = Decimal128(prices[1])

    await EthPrice.set(db)

    token0_derivedETH = await find_eth_per_token(db, token0['tokenAddress'], rpc_url)
    token1_derivedETH = await find_eth_per_token(db, token1['tokenAddress'], rpc_url)
    token0_update_data['$set']['derivedETH'] = Decimal128(token0_derivedETH)
    token1_update_data['$set']['derivedETH'] = Decimal128(token1_derivedETH)
    
    factory_totalValueLockedETH = factory['totalValueLockedETH'].to_decimal() - pool['totalValueLockedETH'].to_decimal()

    eth_price = await EthPrice.get()
    pool_totalValueLockedETH = (pool_totalValueLockedToken0 * token0_derivedETH) + (pool_totalValueLockedToken1 * token1_derivedETH)
    pool_update_data['$set']['totalValueLockedETH'] = Decimal128(pool_totalValueLockedETH)
    pool_update_data['$set']['totalValueLockedUSD'] = Decimal128(pool_totalValueLockedETH * eth_price)

    factory_totalValueLockedETH = factory_totalValueLockedETH + pool_totalValueLockedETH
    factory_update_data['$set']['totalValueLockedETH'] = Decimal128(factory_totalValueLockedETH)
    factory_update_data['$set']['totalValueLockedUSD'] = Decimal128(factory_totalValueLockedETH * eth_price)

    token1_update_data['$set']['totalValueLocked'] = Decimal128(token0_totalValueLocked)
    token1_update_data['$set']['totalValueLockedUSD'] = Decimal128(token0_totalValueLocked * token0_derivedETH * eth_price)

    token1_update_data['$set']['totalValueLocked'] = Decimal128(token1_totalValueLocked)
    token1_update_data['$set']['totalValueLockedUSD'] = Decimal128(token1_totalValueLocked * token1_derivedETH * eth_price)

    EventTracker.pool_addresses_to_update_fee_growth.add(record['poolAddress'])

    await update_tokens_records(db, token0['_id'], token1['_id'], token0_update_data, token1_update_data)
    await update_pool_record(db, pool['_id'], pool_update_data)
    await update_factory_record(db, factory_update_data)

    factory = await get_factory_record(db)
    pool = await get_pool_record(db, record['poolAddress'])
    token0 = await get_token_record(db, token0['tokenAddress'], rpc_url)
    token1 = await get_token_record(db, token1['tokenAddress'], rpc_url)
    
    await update_factory_day_data(db, factory, record['timestamp'], amount_total_ETH_tracked, amount_total_USD_tracked, fees_USD)
    await update_factory_hour_data(db, factory, record['timestamp'], amount_total_ETH_tracked, amount_total_USD_tracked, fees_USD)
    await update_pool_day_data(db, pool, record['timestamp'], amount_total_USD_tracked, amount0_abs, amount1_abs, fees_USD)
    await update_pool_hour_data(db, pool, record['timestamp'], amount_total_USD_tracked, amount0_abs, amount1_abs, fees_USD)
    await update_token_day_data(db, token0, record['timestamp'], amount_total_USD_tracked, amount0_abs, fees_USD)
    await update_token_hour_data(db, token0, record['timestamp'], amount_total_USD_tracked, amount0_abs, fees_USD)
    await update_token_day_data(db, token1, record['timestamp'], amount_total_USD_tracked, amount1_abs, fees_USD)
    await update_token_hour_data(db, token1, record['timestamp'], amount_total_USD_tracked, amount1_abs, fees_USD)
    
    EventTracker.swap_count += 1

async def update_pool_fee_growth(*args, **kwargs):
    db = kwargs['db']
    rpc_url = kwargs['rpc_url']
    for pool_address in EventTracker.pool_addresses_to_update_fee_growth:
        pool_update_data = dict()
        pool_update_data['$set'] = dict()
        pool = await get_pool_record(db, pool_address)
        contract = await Contract.from_address(address=pool_address, provider=FullNodeClient(node_url=rpc_url))
        if contract is not None:
            (fee_growth_global_0_X128,) = await contract.functions["get_fee_growth_global_0_X128"].call()
            pool_update_data['$set']['feeGrowthGlobal0X128'] = hex(fee_growth_global_0_X128)
            (fee_growth_global_1_X128,) = await contract.functions["get_fee_growth_global_1_X128"].call()
            pool_update_data['$set']['feeGrowthGlobal1X128'] = hex(fee_growth_global_1_X128)
            await update_pool_record(db, pool['_id'], pool_update_data)


EVENT_TO_FUNCTION_MAP = {
    Event.INITIALIZE: handle_initialize,
    Event.MINT: handle_mint,
    Event.SWAP: handle_swap,
    Event.BURN: handle_burn,
}


async def process_events(mongo_url: str, mongo_database: Database, rpc_url: str):
    processed_records = []
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        await EthPrice.set(db)
        async for record in yield_pool_data_records(db):
            event_func = EVENT_TO_FUNCTION_MAP.get(record['event'])
            if event_func:
                await event_func(
                    db=db, 
                    record=record,
                    rpc_url=rpc_url)
                processed_records.append(
                    UpdateOne({"_id": record['_id']}, {"$set": {"processed": True}})
                )
        if processed_records:
            db[Collection.POOLS_DATA].bulk_write(processed_records)
        await update_pool_fee_growth(db=db, rpc_url=rpc_url)

    logger.info(f'Successfully processed {EventTracker.initialize_count} Initialize events')
    logger.info(f'Successfully processed {EventTracker.mint_count} Mint events')
    logger.info(f'Successfully processed {EventTracker.swap_count} Swap events')
    logger.info(f'Successfully processed {EventTracker.burn_count} Burn events')
    logger.info(f'Successfully updated {len(EventTracker.pool_addresses_to_update_fee_growth)} pools fee growth')
    EventTracker.initialize_count = 0
    EventTracker.mint_count = 0
    EventTracker.swap_count = 0
    EventTracker.burn_count = 0
    EventTracker.pool_addresses_to_update_fee_growth = set()


async def run_events_transformer(mongo_url: str, mongo_database: Database, rpc_url: str):
    while True:
        await process_events(mongo_url, mongo_database, rpc_url)
        time.sleep(TIME_INTERVAL)
