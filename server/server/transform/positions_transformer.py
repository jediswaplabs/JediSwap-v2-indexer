import time

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from server.const import Collection, Event, ZERO_ADDRESS, DEFAULT_DECIMALS, TIME_INTERVAL
from server.transform.lp_contest_updates import calculate_lp_leaderboard_points
from server.query_utils import get_token_record, get_position_record
from server.utils import amount_after_decimals

from structlog import get_logger


logger = get_logger(__name__)


class EventTracker:
    transfer_count = 0
    increase_liquidity_count = 0
    decrease_liquidity_count = 0
    collect_count = 0


async def update_position_record(db: Database, position_id: str, position_update_data: dict):
    position_query = {'positionId': position_id}
    db[Collection.POSITIONS].update_one(position_query, position_update_data)


async def yield_position_records(db: Database) -> dict:
    query = {
        'ownerAddress': {'$ne': ZERO_ADDRESS},
        '$or': [
            {'processed': {'$exists': False}},
            {'processed': False}
        ]}
    for record in db[Collection.POSITIONS_DATA].find(query):
        yield record


async def handle_transfer(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']

    del record['event']
    logger.info("handle Transfer", **record)

    await get_position_record(db, record['positionId'])

    position_update_data = dict()
    position_update_data['$set'] = dict()
    position_update_data['$set']['positionAddress'] = record['positionAddress']
    position_update_data['$set']['ownerAddress'] = record['ownerAddress']
    position_update_data['$set']['poolFee'] = record['poolFee']
    position_update_data['$set']['tickLower'] = record['tickLower']
    position_update_data['$set']['tickUpper'] = record['tickUpper']
    position_update_data['$set']['token0Address'] = record['token0Address']
    position_update_data['$set']['token1Address'] = record['token1Address']
    position_update_data['$set']['lastUpdatedTimestamp'] = record['timestamp']

    await update_position_record(db, record['positionId'], position_update_data)

    EventTracker.transfer_count += 1


async def handle_increase_liquidity(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']

    del record['event']
    logger.info("handle IncreaseLiquidity", **record)

    token0 = await get_token_record(db, record['token0Address'], rpc_url)
    token1 = await get_token_record(db, record['token1Address'], rpc_url)

    amount0 = await amount_after_decimals(record['depositedToken0'], token0.get('decimals', DEFAULT_DECIMALS))
    amount1 = await amount_after_decimals(record['depositedToken1'], token1.get('decimals', DEFAULT_DECIMALS))

    await calculate_lp_leaderboard_points(record, db, rpc_url, Event.INCREASE_LIQUIDITY)

    position_update_data = {
        '$inc': {
            'liquidity': record['liquidity'],
            'depositedToken0': Decimal128(amount0),
            'depositedToken1': Decimal128(amount1),
        }
    }

    await update_position_record(db, record['positionId'], position_update_data)

    EventTracker.increase_liquidity_count += 1


async def handle_decrease_liquidity(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']

    del record['event']
    logger.info("handle DecreaseLiquidity", **record)

    token0 = await get_token_record(db, record['token0Address'], rpc_url)
    token1 = await get_token_record(db, record['token1Address'], rpc_url)

    amount0 = await amount_after_decimals(record['withdrawnToken0'], token0.get('decimals', DEFAULT_DECIMALS))
    amount1 = await amount_after_decimals(record['withdrawnToken1'], token1.get('decimals', DEFAULT_DECIMALS))

    await calculate_lp_leaderboard_points(record, db, rpc_url, Event.DECREASE_LIQUIDITY)

    position_update_data = {
        '$inc': {
            'liquidity': -record['liquidity'],
            'withdrawnToken0': Decimal128(amount0),
            'withdrawnToken1': Decimal128(amount1),
        }
    }

    await update_position_record(db, record['positionId'], position_update_data)

    EventTracker.decrease_liquidity_count += 1


async def handle_collect(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']

    del record['event']
    logger.info("handle Collect", **record)

    token0 = await get_token_record(db, record['token0Address'], rpc_url)
    token1 = await get_token_record(db, record['token1Address'], rpc_url)

    amount0 = await amount_after_decimals(record['collectedFeesToken0'], token0.get('decimals', DEFAULT_DECIMALS))
    amount1 = await amount_after_decimals(record['collectedFeesToken1'], token1.get('decimals', DEFAULT_DECIMALS))

    position_update_data = {
        '$inc': {
            'collectedFeesToken0': Decimal128(amount0),
            'collectedFeesToken1': Decimal128(amount1),
        }
    }

    await update_position_record(db, record['positionId'], position_update_data)

    EventTracker.collect_count += 1


EVENT_TO_FUNCTION_MAP = {
    Event.TRANSFER: handle_transfer,
    Event.INCREASE_LIQUIDITY: handle_increase_liquidity,
    Event.DECREASE_LIQUIDITY: handle_decrease_liquidity,
    Event.COLLECT: handle_collect,
}


async def process_positions(mongo_url: str, mongo_database: Database, rpc_url: str):
    processed_records = []
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        async for record in yield_position_records(db):
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
            db[Collection.POSITIONS_DATA].bulk_write(processed_records)

    logger.info(f'Successfully processed {EventTracker.transfer_count} Transfer events')
    logger.info(f'Successfully processed {EventTracker.increase_liquidity_count} IncreaseLiquidity events')
    logger.info(f'Successfully processed {EventTracker.decrease_liquidity_count} DecreaseLiquidity events')
    logger.info(f'Successfully processed {EventTracker.collect_count} Collect events')
    EventTracker.transfer_count = 0
    EventTracker.increase_liquidity_count = 0
    EventTracker.decrease_liquidity_count = 0
    EventTracker.collect_count = 0


async def run_positions_transformer(mongo_url: str, mongo_database: Database, rpc_url: str):
    while True:
        await process_positions(mongo_url, mongo_database, rpc_url)
        time.sleep(TIME_INTERVAL)
