from decimal import Decimal

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from server.const import Collection, FACTORY_ADDRESS, ZERO_DECIMAL128
from server.pricing import EthPrice
from server.query_utils import get_pool, get_tokens_from_pool, filter_by_the_latest_value
from server.utils import to_decimal, convert_bigint_field

from pymongo import MongoClient, UpdateOne


class Event:
    MINT = 'Mint'
    BURN = 'Burn'
    SWAP = 'Swap'
    COLLECT = 'Collect'


def update_factory_record(db: Database, factory_totalValueLockedETH: Decimal, pool_totalValueLockedETH: Decimal):
        db[Collection.FACTORIES].update_one({'address': FACTORY_ADDRESS}, {
        '$set': {
            'totalValueLockedUSD': Decimal128((factory_totalValueLockedETH + pool_totalValueLockedETH) * EthPrice.get()),
            'totalValueLockedETH': Decimal128(factory_totalValueLockedETH + pool_totalValueLockedETH)
            },
    })


def yield_pool_data_records(db: Database, event_name: str) -> dict:
    # TODO: get records from a specific pool
    for record in db[Collection.POOLS_DATA].find({
        'event': event_name,
        '$or': [
            {'processed': {'$exists': False}},
            {'processed': False}
        ]}):
        yield record


def get_factory_record(db: Database) -> dict:
    return db[Collection.FACTORIES].find_one({'address': FACTORY_ADDRESS})


def handle_mint(db: Database):
    # TODO: consider adding only one bulk update after records processing
    processed_records = 0
    factory = get_factory_record(db)
    record_status_update_operations = []
    for record in yield_pool_data_records(db, Event.MINT):
        pool = get_pool(db, record['poolAddress'])
        token0, token1 = get_tokens_from_pool(db, pool)
        amount0 = to_decimal(record['amount0'], token0['decimals'])
        amount1 = to_decimal(record['amount1'], token1['decimals'])

        token0_derivedETH = token0['derivedETH'].to_decimal()
        token1_derivedETH = token1['derivedETH'].to_decimal()

        pool_totalValueLockedETH = pool.get('totalValueLockedETH', ZERO_DECIMAL128).to_decimal()
        factory_totalValueLockedETH = factory['totalValueLockedETH'].to_decimal() - pool_totalValueLockedETH

        tokens_update_operations = [
            UpdateOne({"_id": token0['_id']}, {
                "$set": {"totalValueLockedUSD": Decimal128((token0['totalValueLocked'].to_decimal() + amount0) * token0_derivedETH * EthPrice.get())}, 
                "$inc": {"totalValueLocked": Decimal128(amount0)}}),
            UpdateOne({"_id": token1['_id']}, {
                "$set": {"totalValueLockedUSD": Decimal128((token1['totalValueLocked'].to_decimal() + amount1) * token1_derivedETH * EthPrice.get())}, 
                "$inc": {"totalValueLocked": Decimal128(amount1)}}),
        ]
        db[Collection.TOKENS].bulk_write(tokens_update_operations)

        pool_tick = pool.get('tick')
        if pool_tick:
            pool_tick = convert_bigint_field(pool_tick)

        pool_update_data = {'$inc': {}}
        if (pool_tick is not None and
                convert_bigint_field(record['tickLower']) <= pool_tick < convert_bigint_field(record['tickUpper'])):
            pool_update_data["$inc"]["liquidity"] = Decimal128(record['amount'])

        pool_totalValueLockedToken0 = pool.get('totalValueLockedToken0', ZERO_DECIMAL128).to_decimal()
        pool_totalValueLockedToken1 = pool.get('pool_totalValueLockedToken1', ZERO_DECIMAL128).to_decimal()
        pool_totalValueLockedETH = ((pool_totalValueLockedToken0 + amount0) * token0_derivedETH) + (
            (pool_totalValueLockedToken1 + amount1) * token1_derivedETH)
        
        pool_update_data["$inc"]['totalValueLockedToken0'] = Decimal128(amount0)
        pool_update_data["$inc"]['totalValueLockedToken1'] = Decimal128(amount1)
        pool_update_data["$set"] = {
            'totalValueLockedETH': Decimal128(pool_totalValueLockedETH),
            'totalValueLockedUSD': Decimal128(pool_totalValueLockedETH * EthPrice.get()),
        }

        pool_query = {"_id": pool['_id']}
        filter_by_the_latest_value(pool_query)
        db[Collection.POOLS].update_one(pool_query, pool_update_data)

        update_factory_record(db, factory_totalValueLockedETH, pool_totalValueLockedETH)

        record_status_update_operations.append(
            UpdateOne({"_id": record['_id']}, {"$set": {"processed": True}})
        )
        processed_records += 1

    if record_status_update_operations:
        db[Collection.POOLS_DATA].bulk_write(record_status_update_operations)
    print(f'Successfully processed {processed_records} Mint events')


def handle_burn(db: Database):
    # TODO: consider adding only one bulk update after records processing
    processed_records = 0
    factory = get_factory_record(db)
    record_status_update_operations = []
    for record in yield_pool_data_records(db, Event.BURN):
        pool = get_pool(db, record['poolAddress'])
        token0, token1 = get_tokens_from_pool(db, pool)
        amount0 = to_decimal(record['amount0'], token0['decimals'])
        amount1 = to_decimal(record['amount1'], token1['decimals'])

        token0_derivedETH = token0['derivedETH'].to_decimal()
        token1_derivedETH = token1['derivedETH'].to_decimal()

        pool_totalValueLockedETH = pool.get('totalValueLockedETH', ZERO_DECIMAL128).to_decimal()
        factory_totalValueLockedETH = factory['totalValueLockedETH'].to_decimal() - pool_totalValueLockedETH

        tokens_update_operations = [
            UpdateOne({"_id": token0['_id']}, {
                "$set": {"totalValueLockedUSD": Decimal128((token0['totalValueLocked'].to_decimal() - amount0) * token0_derivedETH * EthPrice.get())}, 
                "$inc": {"totalValueLocked": Decimal128(-amount0)}}),
            UpdateOne({"_id": token1['_id']}, {
                "$set": {"totalValueLockedUSD": Decimal128((token1['totalValueLocked'].to_decimal() - amount1) * token1_derivedETH * EthPrice.get())}, 
                "$inc": {"totalValueLocked": Decimal128(-amount1)}}),
        ]
        db[Collection.TOKENS].bulk_write(tokens_update_operations)

        pool_tick = pool.get('tick')
        if pool_tick:
            pool_tick = convert_bigint_field(pool_tick)

        pool_update_data = {'$inc': {}}
        if (pool_tick is not None and
                convert_bigint_field(record['tickLower']) <= pool_tick < convert_bigint_field(record['tickUpper'])):
            pool_update_data["$inc"]["liquidity"] = Decimal128(-record['amount'])

        pool_totalValueLockedToken0 = pool.get('totalValueLockedToken0', ZERO_DECIMAL128).to_decimal()
        pool_totalValueLockedToken1 = pool.get('pool_totalValueLockedToken1', ZERO_DECIMAL128).to_decimal()
        pool_totalValueLockedETH = ((pool_totalValueLockedToken0 - amount0) * token0_derivedETH) + (
            (pool_totalValueLockedToken1 - amount1) * token1_derivedETH)
        
        pool_update_data["$inc"]['totalValueLockedToken0'] = Decimal128(-amount0)
        pool_update_data["$inc"]['totalValueLockedToken1'] = Decimal128(-amount1)
        pool_update_data["$set"] = {
            'totalValueLockedETH': Decimal128(pool_totalValueLockedETH),
            'totalValueLockedUSD': Decimal128(pool_totalValueLockedETH * EthPrice.get()),
        }

        pool_query = {"_id": pool['_id']}
        filter_by_the_latest_value(pool_query)
        db[Collection.POOLS].update_one(pool_query, pool_update_data)

        update_factory_record(db, factory_totalValueLockedETH, pool_totalValueLockedETH)

        record_status_update_operations.append(
            UpdateOne({"_id": record['_id']}, {"$set": {"processed": True}})
        )
        processed_records += 1

    if record_status_update_operations:
        db[Collection.POOLS_DATA].bulk_write(record_status_update_operations)
    print(f'Successfully processed {processed_records} Burn events')


def run(mongo_url: str, mongo_database: Database, rpc_url: str):
    EthPrice.set(rpc_url)
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        handle_mint(db)
        handle_burn(db)
