import os
import sys

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from utils import to_decimal, convert_bigint_field, get_pool, get_tokens_from_pool, filter_by_the_latest_value
from const import Collection


class Event:
    MINT = 'Mint'
    BURN = 'Burn'
    SWAP = 'Swap'
    COLLECT = 'Collect'


def yield_record(db: Database, event_name: str) -> dict:
    # TODO: get from DB only not processed records
    pools_data_collection = db[Collection.POOLS_DATA]
    for record in pools_data_collection.find({'event': event_name}):
        yield record


def handle_mint(db):
    # TODO: consider adding only one bulk update after records processing
    for record in yield_record(db, Event.MINT):
        pool = get_pool(db, record['poolAddress'])
        token0, token1 = get_tokens_from_pool(db, pool)
        amount0 = to_decimal(record['amount0'], token0['decimals'])
        amount1 = to_decimal(record['amount1'], token1['decimals'])
        update_operations = [
            UpdateOne({"_id": token0['_id']}, {"$inc": {"totalValueLocked": Decimal128(amount0)}}),
            UpdateOne({"_id": token1['_id']}, {"$inc": {"totalValueLocked": Decimal128(amount1)}})
        ]
        db[Collection.TOKENS].bulk_write(update_operations)

        pool_tick = pool.get('tick')
        if pool_tick:
            pool_tick = convert_bigint_field(pool_tick)

        if (pool_tick is not None and
                convert_bigint_field(record['tickLower']) <= pool_tick < convert_bigint_field(record['tickUpper'])):
            query = {"_id": pool['_id']}
            filter_by_the_latest_value(query)
            db[Collection.POOLS].update_one(
                query,
                {"$inc": {"liquidity": record['amount']}},
            )
        
        # TODO: add fields:
        # tokens collection: totalValueLockedUSD
        # pools collections: totalValueLockedUSD  
        # factory collection: totalValueLockedUSD 
    print('Mint records successfully proceed')


def run():
    mongo_url = os.environ.get('MONGO_URL', None)
    if mongo_url is None:
        sys.exit('MONGO_URL not set')
    mongo_database = os.environ.get('MONGO_DB', None)
    if mongo_database is None:
        sys.exit('MONGO_DB not set')

    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        handle_mint(db)
