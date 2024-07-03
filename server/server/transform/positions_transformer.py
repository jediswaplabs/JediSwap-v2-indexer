import time

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from server.const import Collection, Event, ZERO_ADDRESS, DEFAULT_DECIMALS, TIME_INTERVAL, ZERO_DECIMAL
from server.transform.lp_contest_updates import (
    insert_lp_leaderboard_snapshot, 
    process_position_for_lp_leaderboard_for_position_transformer, 
    insert_lp_leaderboard_snapshot_collect_event
)
from server.query_utils import get_token_record, get_position_record, get_teahouse_position_record
from server.utils import amount_after_decimals

from structlog import get_logger


logger = get_logger(__name__)


class EventTracker:
    transfer_count = 0
    increase_liquidity_count = 0
    decrease_liquidity_count = 0
    collect_count = 0
    teahouse_add_liquidity_count = 0
    teahouse_remove_liquidity_count = 0
    teahouse_collect_count = 0


async def update_position_record(db: Database, position_id: str, position_update_data: dict):
    position_query = {'positionId': position_id}
    db[Collection.POSITIONS].update_one(position_query, position_update_data)


async def yield_position_records(db: Database, collection: str) -> dict:
    query = {
        'ownerAddress': {'$ne': ZERO_ADDRESS},
        '$or': [
            {'processed': {'$exists': False}},
            {'processed': False}
        ]}
    for record in db[collection].find(query):
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

    await insert_lp_leaderboard_snapshot(record, db, Event.INCREASE_LIQUIDITY)

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

    position_record = await get_position_record(db, record['positionId'])
    await insert_lp_leaderboard_snapshot(record, db, Event.DECREASE_LIQUIDITY, position_record)

    missing_block, records_to_be_inserted = await process_position_for_lp_leaderboard_for_position_transformer(
        db, record, position_record)
    if not missing_block:
        for pos_record in records_to_be_inserted:
            await insert_lp_leaderboard_snapshot(pos_record['event_data'], db, position_record=pos_record['position_record'])

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
    token0_decimals = token0.get('decimals', DEFAULT_DECIMALS)
    token1_decimals = token1.get('decimals', DEFAULT_DECIMALS)

    collected_amount0 = await amount_after_decimals(record['collectedFeesToken0'], token0_decimals)
    collected_amount1 = await amount_after_decimals(record['collectedFeesToken1'], token1_decimals)

    withdrawn_amount0 = ZERO_DECIMAL
    withdrawn_amount1 = ZERO_DECIMAL
    if decrease_liquidity_record := db[Collection.POSITIONS_DATA].find_one({
        'positionId': record['positionId'],
        'event': Event.DECREASE_LIQUIDITY,
        'timestamp': record['timestamp'],
    }):
        withdrawn_amount0 = await amount_after_decimals(decrease_liquidity_record['withdrawnToken0'], token0_decimals)
        withdrawn_amount1 = await amount_after_decimals(decrease_liquidity_record['withdrawnToken1'], token1_decimals)

    amount0 = collected_amount0 - withdrawn_amount0
    amount1 = collected_amount1 - withdrawn_amount1

    position_update_data = {
        '$inc': {
            'collectedFeesToken0': Decimal128(amount0),
            'collectedFeesToken1': Decimal128(amount1),
        }
    }

    await update_position_record(db, record['positionId'], position_update_data)

    position_record = await get_position_record(db, record['positionId'])
    await insert_lp_leaderboard_snapshot_collect_event(record, db, position_record=position_record,
                                                       amount0_fees=amount0, amount1_fees=amount1)

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
        async for record in yield_position_records(db, Collection.POSITIONS_DATA):
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


async def update_teahouse_position_record(db: Database, pool_address: str, position_update_data: dict):
    position_query = {
        'poolAddress': pool_address,
    }
    db[Collection.TEAHOUSE_VAULT].update_one(position_query, position_update_data)


async def handle_teahouse_add_liquidity(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']

    del record['event']
    logger.info("handle Teahouse AddLiquidity", **record)

    position_record = await get_teahouse_position_record(db, record, rpc_url)

    token0 = await get_token_record(db, position_record['token0Address'], rpc_url)
    token1 = await get_token_record(db, position_record['token1Address'], rpc_url)

    amount0 = await amount_after_decimals(record['depositedToken0'], token0.get('decimals', DEFAULT_DECIMALS))
    amount1 = await amount_after_decimals(record['depositedToken1'], token1.get('decimals', DEFAULT_DECIMALS))

    position_record.update({
        'tickLower': record['tickLower'],
        'tickUpper': record['tickUpper'],
    })
    await insert_lp_leaderboard_snapshot(record, db, Event.INCREASE_LIQUIDITY, position_record, teahouse=True)

    position_update_data = dict()
    position_update_data['$inc'] = dict()
    position_update_data['$inc']['liquidity'] = record['liquidity']
    position_update_data['$inc']['depositedToken0'] = Decimal128(amount0)
    position_update_data['$inc']['depositedToken1'] = Decimal128(amount1)
    position_update_data['$set'] = dict()
    position_update_data['$set']['tickLower'] = record['tickLower']
    position_update_data['$set']['tickUpper'] = record['tickUpper']
    position_update_data['$set']['ownerAddress'] = record['tx_sender']

    await update_teahouse_position_record(db, record['poolAddress'], position_update_data)

    EventTracker.teahouse_add_liquidity_count += 1


async def handle_teahouse_remove_liquidity(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']

    del record['event']
    logger.info("handle Teahouse RemoveLiquidity", **record)

    position_record = await get_teahouse_position_record(db, record, rpc_url)

    token0 = await get_token_record(db, position_record['token0Address'], rpc_url)
    token1 = await get_token_record(db, position_record['token1Address'], rpc_url)

    amount0 = await amount_after_decimals(record['withdrawnToken0'], token0.get('decimals', DEFAULT_DECIMALS))
    amount1 = await amount_after_decimals(record['withdrawnToken1'], token1.get('decimals', DEFAULT_DECIMALS))

    new_position_record = position_record.copy()
    new_position_record.update({
        'tickLower': record['tickLower'],
        'tickUpper': record['tickUpper'],
    })
    await insert_lp_leaderboard_snapshot(record, db, Event.DECREASE_LIQUIDITY, new_position_record, teahouse=True)

    missing_block, records_to_be_inserted = await process_position_for_lp_leaderboard_for_position_transformer(
        db, record, position_record)
    if not missing_block:
        for pos_record in records_to_be_inserted:
            await insert_lp_leaderboard_snapshot(pos_record['event_data'], db, position_record=pos_record['position_record'], teahouse=True)

    position_update_data = dict()
    position_update_data['$inc'] = dict()
    position_update_data['$inc']['liquidity'] = -record['liquidity']
    position_update_data['$inc']['withdrawnToken0'] = Decimal128(amount0)
    position_update_data['$inc']['withdrawnToken1'] = Decimal128(amount1)
    position_update_data['$set'] = dict()
    position_update_data['$set']['tickLower'] = record['tickLower']
    position_update_data['$set']['tickUpper'] = record['tickUpper']
    position_update_data['$set']['ownerAddress'] = record['tx_sender']

    await update_teahouse_position_record(db, record['poolAddress'], position_update_data)

    EventTracker.teahouse_remove_liquidity_count += 1


async def handle_teahouse_collect(*args, **kwargs):
    db = kwargs['db']
    record = kwargs['record']
    rpc_url = kwargs['rpc_url']

    del record['event']
    logger.info("handle Teahouse Collect", **record)

    if db[Collection.TEAHOUSE_VAULT_DATA].find_one({
        'poolAddress': record['poolAddress'],
        'tx_sender': record['tx_sender'],
        'event': Event.REMOVE_LIQUIDITY,
        'timestamp': record['timestamp'],
        'withdrawnToken0': record['collectedFeesToken0'],
        'withdrawnToken1': record['collectedFeesToken1'],
    }):
        # skip Collect events that have the same amount of collected fees as withdrawn tokens for RemoveLiquidity event 
        EventTracker.teahouse_collect_count += 1
        return
    
    position_record = await get_teahouse_position_record(db, record, rpc_url)

    token0 = await get_token_record(db, position_record['token0Address'], rpc_url)
    token1 = await get_token_record(db, position_record['token1Address'], rpc_url)

    token0_decimals = token0.get('decimals', DEFAULT_DECIMALS)
    token1_decimals = token1.get('decimals', DEFAULT_DECIMALS)

    collected_amount0 = await amount_after_decimals(record['collectedFeesToken0'], token0_decimals)
    collected_amount1 = await amount_after_decimals(record['collectedFeesToken1'], token1_decimals)

    position_update_data = dict()
    position_update_data['$inc'] = dict()
    position_update_data['$inc']['collectedFeesToken0'] = Decimal128(collected_amount0)
    position_update_data['$inc']['collectedFeesToken1'] = Decimal128(collected_amount1)

    await update_teahouse_position_record(db, record['poolAddress'], position_update_data)

    position_record = await get_teahouse_position_record(db, record, rpc_url)
    await insert_lp_leaderboard_snapshot_collect_event(record, db, position_record=position_record, teahouse=True,
                                                       amount0_fees=collected_amount0, amount1_fees=collected_amount1)

    EventTracker.teahouse_collect_count += 1


TEAHOUSE_EVENT_TO_FUNCTION_MAP = {
    Event.ADD_LIQUIDITY: handle_teahouse_add_liquidity,
    Event.REMOVE_LIQUIDITY: handle_teahouse_remove_liquidity,
    Event.COLLECT: handle_teahouse_collect,
}


async def process_teahouse_positions(mongo_url: str, mongo_database: Database, rpc_url: str):
    processed_records = []
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        async for record in yield_position_records(db, Collection.TEAHOUSE_VAULT_DATA):
            event_func = TEAHOUSE_EVENT_TO_FUNCTION_MAP.get(record['event'])
            if event_func:
                await event_func(
                    db=db, 
                    record=record,
                    rpc_url=rpc_url)
                processed_records.append(
                    UpdateOne({"_id": record['_id']}, {"$set": {"processed": True}})
                )
        if processed_records:
            db[Collection.TEAHOUSE_VAULT_DATA].bulk_write(processed_records)

    logger.info(f"Successfully processed {EventTracker.teahouse_add_liquidity_count} Teahouse's AddLiquidity events")
    logger.info(f"Successfully processed {EventTracker.teahouse_remove_liquidity_count} Teahouse's RemoveLiquidity events")
    logger.info(f"Successfully processed {EventTracker.teahouse_collect_count} Teahouse's Collect events")
    EventTracker.teahouse_add_liquidity_count = 0
    EventTracker.teahouse_remove_liquidity_count = 0
    EventTracker.teahouse_collect_count = 0


async def run_positions_transformer(mongo_url: str, mongo_database: Database, rpc_url: str):
    while True:
        await process_positions(mongo_url, mongo_database, rpc_url)
        await process_teahouse_positions(mongo_url, mongo_database, rpc_url)
        time.sleep(TIME_INTERVAL)
