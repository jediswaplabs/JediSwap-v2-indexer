from decimal import Decimal

from bson import Decimal128
from pymongo.database import Database

from server.const import Collection, ZERO_DECIMAL128, ZERO_DECIMAL
from server.pricing import EthPrice

from pymongo import UpdateOne


def get_day_id(timestamp: str) -> tuple[int, int]:
    day_id = int(timestamp) // 86400
    day_start_timestamp = day_id * 86400
    return day_id, day_start_timestamp


def get_hour_id(timestamp: str) -> tuple[int, int]:
    hour_id = int(timestamp) // 3600
    hour_start_timestamp = hour_id * 3600
    return hour_id, hour_start_timestamp


def update_factory_day_data(db: Database, factory_record: dict, timestamp: str, 
                            amount_total_ETH_tracked: Decimal = ZERO_DECIMAL, 
                            amount_total_USD_tracked: Decimal = ZERO_DECIMAL, 
                            fees_USD: Decimal = ZERO_DECIMAL):
    day_id, day_start_timestamp = get_day_id(timestamp)

    factory_day_data_record = db[Collection.FACTORIES_DAY_DATA].find_one({'dayId': day_id})
    if not factory_day_data_record:
        factory_day_data_record = {
            'dayId': day_id,
            'date': day_start_timestamp,
            'volumeETH': ZERO_DECIMAL128,
            'volumeUSD': ZERO_DECIMAL128,
            'feesUSD': ZERO_DECIMAL128,
        }
    factory_day_data_record['totalValueLockedUSD'] = factory_record['totalValueLockedUSD']
    factory_day_data_record['txCount'] = factory_record['txCount']
    factory_day_data_record['volumeETH'] = Decimal128(factory_day_data_record['volumeETH'].to_decimal() + amount_total_ETH_tracked)
    factory_day_data_record['volumeUSD'] = Decimal128(factory_day_data_record['volumeUSD'].to_decimal() + amount_total_USD_tracked)
    factory_day_data_record['feesUSD'] = Decimal128(factory_day_data_record['feesUSD'].to_decimal() + fees_USD)

    update_request = UpdateOne({
        'dayId': day_id
        }, {
            '$set': factory_day_data_record
        }, upsert=True)
    db[Collection.FACTORIES_DAY_DATA].bulk_write([update_request])
    return factory_day_data_record


def update_pool_day_data(db: Database, pool_record: dict, timestamp: str, 
                         amount_total_USD_tracked: Decimal = ZERO_DECIMAL, 
                         amount0_abs: Decimal = ZERO_DECIMAL, 
                         amount1_abs: Decimal = ZERO_DECIMAL, 
                         fees_USD: Decimal = ZERO_DECIMAL):
    day_id, day_start_timestamp = get_day_id(timestamp)

    pool_day_data_record = db[Collection.POOLS_DAY_DATA].find_one({
        'poolAddress': pool_record['poolAddress'],
        'dayId': day_id,
    })
    if not pool_day_data_record:
        pool_day_data_record = {
            'dayId': day_id,
            'date': day_start_timestamp,
            'poolAddress': pool_record['poolAddress'],
            'volumeToken0': ZERO_DECIMAL128,
            'volumeToken1': ZERO_DECIMAL128,
            'volumeUSD': ZERO_DECIMAL128,
            'feesUSD': ZERO_DECIMAL128,
            'txCount': 0,
            'feeGrowthGlobal0X128': ZERO_DECIMAL128,
            'feeGrowthGlobal1X128': ZERO_DECIMAL128,
            'open': pool_record['token0Price'],
            'high': pool_record['token0Price'],
            'low': pool_record['token0Price'],
            'close': pool_record['token0Price'],
        }

    if pool_record['token0Price'].to_decimal() > pool_day_data_record['high'].to_decimal():
        pool_day_data_record['high'] = pool_record['token0Price']

    if pool_record['token0Price'].to_decimal() < pool_day_data_record['low'].to_decimal():
        pool_day_data_record['low'] = pool_record['token0Price']

    pool_day_data_record['liquidity'] = pool_record['liquidity']
    pool_day_data_record['sqrtPriceX96'] = pool_record['sqrtPriceX96']
    pool_day_data_record['feeGrowthGlobal0X128'] = pool_record['feeGrowthGlobal0X128']
    pool_day_data_record['feeGrowthGlobal1X128'] = pool_record['feeGrowthGlobal1X128']
    pool_day_data_record['token0Price'] = pool_record['token0Price']
    pool_day_data_record['token1Price'] = pool_record['token1Price']
    pool_day_data_record['tick'] = pool_record['tick']
    pool_day_data_record['totalValueLockedUSD'] = pool_record['totalValueLockedUSD']
    pool_day_data_record['txCount'] += 1
    pool_day_data_record['volumeUSD'] = Decimal128(pool_day_data_record['volumeUSD'].to_decimal() + amount_total_USD_tracked)
    pool_day_data_record['volumeToken0'] = Decimal128(pool_day_data_record['volumeToken0'].to_decimal() + amount0_abs)
    pool_day_data_record['volumeToken1'] = Decimal128(pool_day_data_record['volumeToken1'].to_decimal() + amount1_abs)
    pool_day_data_record['feesUSD'] = Decimal128(pool_day_data_record['feesUSD'].to_decimal() + fees_USD)

    update_request = UpdateOne({
        'poolAddress': pool_record['poolAddress'],
        'dayId': day_id,
        }, {
            '$set': pool_day_data_record,
        }, upsert=True)
    db[Collection.POOLS_DAY_DATA].bulk_write([update_request])
    return pool_day_data_record
  

def update_pool_hour_data(db: Database, pool_record: dict, timestamp: str, 
                          amount_total_USD_tracked: Decimal = ZERO_DECIMAL, 
                          amount0_abs: Decimal = ZERO_DECIMAL, 
                          amount1_abs: Decimal = ZERO_DECIMAL, 
                          fees_USD: Decimal = ZERO_DECIMAL):
    hour_id, hour_start_timestamp = get_hour_id(timestamp)

    pool_hour_data_record = db[Collection.POOLS_HOUR_DATA].find_one({
        'poolAddress': pool_record['poolAddress'],
        'hourId': hour_id,
    })
    if not pool_hour_data_record:
        pool_hour_data_record = {
            'hourId': hour_id,
            'periodStartUnix': hour_start_timestamp,
            'poolAddress': pool_record['poolAddress'],
            'volumeToken0': ZERO_DECIMAL128,
            'volumeToken1': ZERO_DECIMAL128,
            'volumeUSD': ZERO_DECIMAL128,
            'feesUSD': ZERO_DECIMAL128,
            'txCount': 0,
            'feeGrowthGlobal0X128': ZERO_DECIMAL128,
            'feeGrowthGlobal1X128': ZERO_DECIMAL128,
            'open': pool_record['token0Price'],
            'high': pool_record['token0Price'],
            'low': pool_record['token0Price'],
            'close': pool_record['token0Price'],
        }

    if pool_record['token0Price'].to_decimal() > pool_hour_data_record['high'].to_decimal():
        pool_hour_data_record['high'] = pool_record['token0Price']

    if pool_record['token0Price'].to_decimal() < pool_hour_data_record['low'].to_decimal():
        pool_hour_data_record['low'] = pool_record['token0Price']

    pool_hour_data_record['liquidity'] = pool_record['liquidity']
    pool_hour_data_record['sqrtPriceX96'] = pool_record['sqrtPriceX96']
    pool_hour_data_record['feeGrowthGlobal0X128'] = pool_record['feeGrowthGlobal0X128']
    pool_hour_data_record['feeGrowthGlobal1X128'] = pool_record['feeGrowthGlobal1X128']
    pool_hour_data_record['token0Price'] = pool_record['token0Price']
    pool_hour_data_record['token1Price'] = pool_record['token1Price']
    pool_hour_data_record['tick'] = pool_record['tick']
    pool_hour_data_record['totalValueLockedUSD'] = pool_record['totalValueLockedUSD']
    pool_hour_data_record['txCount'] += 1
    pool_hour_data_record['volumeUSD'] = Decimal128(pool_hour_data_record['volumeUSD'].to_decimal() + amount_total_USD_tracked)
    pool_hour_data_record['volumeToken0'] = Decimal128(pool_hour_data_record['volumeToken0'].to_decimal() + amount0_abs)
    pool_hour_data_record['volumeToken1'] = Decimal128(pool_hour_data_record['volumeToken1'].to_decimal() + amount1_abs)
    pool_hour_data_record['feesUSD'] = Decimal128(pool_hour_data_record['feesUSD'].to_decimal() + fees_USD)

    update_request = UpdateOne({
        'poolAddress': pool_record['poolAddress'],
        'hourId': hour_id,
        }, {
            '$set': pool_hour_data_record,
        }, upsert=True)
    db[Collection.POOLS_HOUR_DATA].bulk_write([update_request])
    return pool_hour_data_record


def update_token_day_data(db: Database, token_record: dict, timestamp: str, 
                          amount_total_USD_tracked: Decimal = ZERO_DECIMAL, 
                          amount_abs: Decimal = ZERO_DECIMAL, 
                          fees_USD: Decimal = ZERO_DECIMAL):
    day_id, day_start_timestamp = get_day_id(timestamp)
    token_price = token_record['derivedETH'].to_decimal() * EthPrice.get()

    token_day_data_record = db[Collection.TOKENS_DAY_DATA].find_one({
        'tokenAddress': token_record['tokenAddress'],
        'dayId': day_id,
    })
    if not token_day_data_record:
        token_day_data_record = {
            'dayId': day_id,
            'date': day_start_timestamp,
            'tokenAddress': token_record['tokenAddress'],
            'volume': ZERO_DECIMAL128,
            'volumeUSD': ZERO_DECIMAL128,
            'feesUSD': ZERO_DECIMAL128,
            'untrackedVolumeUSD': ZERO_DECIMAL128,
            'open': Decimal128(token_price),
            'high': Decimal128(token_price),
            'low': Decimal128(token_price),
            'close': Decimal128(token_price),
        }

    if token_price > token_day_data_record['high'].to_decimal():
        token_day_data_record['high'] = Decimal128(token_price)

    if token_price < token_day_data_record['low'].to_decimal():
        token_day_data_record['low'] = Decimal128(token_price)

    token_day_data_record['close'] = Decimal128(token_price)
    token_day_data_record['priceUSD'] = Decimal128(token_price)
    token_day_data_record['totalValueLocked'] = token_record['totalValueLocked']
    token_day_data_record['totalValueLockedUSD'] = token_record['totalValueLockedUSD']
    token_day_data_record['volume'] = Decimal128(token_day_data_record['volume'].to_decimal() + amount_total_USD_tracked)
    token_day_data_record['volumeUSD'] = Decimal128(token_day_data_record['volumeUSD'].to_decimal() + amount_abs)
    token_day_data_record['untrackedVolumeUSD'] = Decimal128(token_day_data_record['untrackedVolumeUSD'].to_decimal() + amount_abs)
    token_day_data_record['feesUSD'] = Decimal128(token_day_data_record['feesUSD'].to_decimal() + fees_USD)

    update_request = UpdateOne({
        'tokenAddress': token_record['tokenAddress'],
        'dayId': day_id,
        }, {
            '$set': token_day_data_record
        }, upsert=True)
    db[Collection.TOKENS_DAY_DATA].bulk_write([update_request])
    return token_day_data_record


def update_token_hour_data(db: Database, token_record: dict, timestamp: str, 
                           amount_total_USD_tracked: Decimal = ZERO_DECIMAL, 
                           amount_abs: Decimal = ZERO_DECIMAL, 
                           fees_USD: Decimal = ZERO_DECIMAL):
    hour_id, hour_start_timestamp = get_hour_id(timestamp)
    token_price = token_record['derivedETH'].to_decimal() * EthPrice.get()

    token_hour_data_record = db[Collection.TOKENS_HOUR_DATA].find_one({
        'tokenAddress': token_record['tokenAddress'],
        'hourId': hour_id,
    })
    if not token_hour_data_record:
        token_hour_data_record = {
            'hourId': hour_id,
            'periodStartUnix': hour_start_timestamp,
            'tokenAddress': token_record['tokenAddress'],
            'volume': ZERO_DECIMAL128,
            'volumeUSD': ZERO_DECIMAL128,
            'feesUSD': ZERO_DECIMAL128,
            'untrackedVolumeUSD': ZERO_DECIMAL128,
            'open': Decimal128(token_price),
            'high': Decimal128(token_price),
            'low': Decimal128(token_price),
            'close': Decimal128(token_price),
        }

    if token_price > token_hour_data_record['high'].to_decimal():
        token_hour_data_record['high'] = Decimal128(token_price)

    if token_price < token_hour_data_record['low'].to_decimal():
        token_hour_data_record['low'] = Decimal128(token_price)

    token_hour_data_record['close'] = Decimal128(token_price)
    token_hour_data_record['priceUSD'] = Decimal128(token_price)
    token_hour_data_record['totalValueLocked'] = token_record['totalValueLocked']
    token_hour_data_record['totalValueLockedUSD'] = token_record['totalValueLockedUSD']
    token_hour_data_record['volume'] = Decimal128(token_hour_data_record['volume'].to_decimal() + amount_total_USD_tracked)
    token_hour_data_record['volumeUSD'] = Decimal128(token_hour_data_record['volumeUSD'].to_decimal() + amount_abs)
    token_hour_data_record['untrackedVolumeUSD'] = Decimal128(token_hour_data_record['untrackedVolumeUSD'].to_decimal() + amount_abs)
    token_hour_data_record['feesUSD'] = Decimal128(token_hour_data_record['feesUSD'].to_decimal() + fees_USD)

    update_request = UpdateOne({
        'tokenAddress': token_record['tokenAddress'],
        'hourId': hour_id,
        }, {
            '$set': token_hour_data_record
        }, upsert=True)
    db[Collection.TOKENS_HOUR_DATA].bulk_write([update_request])
    return token_hour_data_record
