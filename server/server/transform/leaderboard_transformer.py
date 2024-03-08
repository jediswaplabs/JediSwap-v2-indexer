from decimal import Decimal
from collections import defaultdict
import os
import time

from bson import Decimal128
from pymongo import MongoClient, UpdateOne, InsertOne
from pymongo.database import Database

from server.const import Collection, ZERO_DECIMAL, ZERO_DECIMAL128, MAX_UINT128, ETH, USDC, USDT, STRK
from server.query_utils import filter_by_the_latest_value, get_pool_by_tokens, simulate_tx, \
    get_position_fee_by_position_id
from server.utils import get_hour_id, get_day_id, format_address, amount_after_decimals

from starknet_py.contract import ContractFunction
from starknet_py.net.client_models import TransactionType
from starknet_py.net.full_node_client import FullNodeClient
from structlog import get_logger

logger = get_logger(__name__)

DAY_INTERVAL = 86400  # 1 day
LP_CONST = Decimal(0.8)


def get_pool_boost(token0_address: str, token1_address) -> Decimal:
    if {token0_address, token1_address} == {ETH, USDC}:
        return Decimal(2)
    elif {token0_address, token1_address} == {USDC, USDT}:
        return Decimal(2)
    elif {token0_address, token1_address} == {STRK, ETH}:
        return Decimal(3)
    elif {token0_address, token1_address} == {STRK, USDC}:
        return Decimal(3)
    else:
        return Decimal(1)


def get_streak_boost(streak_level: int) -> Decimal:
    if streak_level >= 60:
        return Decimal(1.5)
    elif 30 <= streak_level <= 59:
        return Decimal(1.3)
    elif 15 <= streak_level <= 29:
        return Decimal(1.2)
    elif 7 <= streak_level <= 14:
        return Decimal(1.1)
    else:
        return Decimal(1)


class CollectTx:
    METHOD = 'collect'
    TOTAL_TX = '0x1'
    COLLECT_OFFSET = '0x0'
    COLLECT_CALL_DATA_LENGTH = '0x05'
    TOTAL_CALL_DATA_LENGTH = '0x5'

    def __init__(self, position_record: dict, contract_address: str, nonce: int, braavos_account: bool = False) -> None:
        recipient = format_address(position_record['ownerAddress'])
        method_calldata = [
            position_record['positionId'],
            '0',  # position_id is uint256
            recipient,
            MAX_UINT128,
            MAX_UINT128,
        ]

        if braavos_account:
            calldata = [
                self.TOTAL_TX,
                format_address(contract_address),
                ContractFunction.get_selector(self.METHOD),
                self.COLLECT_OFFSET,
                self.COLLECT_CALL_DATA_LENGTH,
                self.TOTAL_CALL_DATA_LENGTH
            ]
        else:
            calldata = [
                self.TOTAL_TX,
                format_address(contract_address),
                ContractFunction.get_selector(self.METHOD),
                self.COLLECT_CALL_DATA_LENGTH,
            ]

        self.sender_address = recipient
        self.type = TransactionType.INVOKE
        self.calldata = calldata + method_calldata
        self.max_fee = int(1e16)
        self.version = 1
        self.signature = ['0x1', '0x2']
        self.nonce = nonce


async def simulate_collect_tx(rpc_url: str, position_record: dict) -> tuple[Decimal, Decimal]:
    rpc = FullNodeClient(node_url=rpc_url)
    nonce = await rpc.get_contract_nonce(format_address(position_record['ownerAddress']))

    for nft_contract in os.environ['NFT_ROUTER_CONTRACTS'].split(','):
        for is_braavos_account in [False, True]:
            collect_tx = CollectTx(position_record, nft_contract, nonce, is_braavos_account)
            result = await simulate_tx(collect_tx, rpc_url)
            if result:
                return result[-2], result[-1]
    else:
        logger.warn(f"Couldn't fetch fees from position: {position_record['positionId']}")
        return ZERO_DECIMAL, ZERO_DECIMAL


async def get_pool_volume_by_day_id(db: Database, day_id: int, pool_address: dict) -> Decimal:
    query = {
        'dayId': day_id,
        'poolAddress': pool_address,
    }
    pool_day_data = db[Collection.POOLS_DAY_DATA].find_one(query)
    if not pool_day_data:
        logger.info(f'Pool volume for {pool_address} and day id {day_id} not found')
        pool_day_data = {}
    return pool_day_data.get('volumeUSD', ZERO_DECIMAL128).to_decimal()


async def get_token_price_by_hour_id(db: Database, hour_id: int, token_address: str) -> Decimal:
    query = {
        'hourId': hour_id,
        'tokenAddress': token_address,
    }
    token_hour_data = db[Collection.TOKENS_HOUR_DATA].find_one(query)
    if not token_hour_data:
        logger.info(f'Token price for {token_address} and hourd id {hour_id} not found')
        token_hour_data = {}
    return token_hour_data.get('priceUSD', ZERO_DECIMAL128).to_decimal()


async def yield_position_records(db: Database, points_calculation: bool = False) -> dict:
    if points_calculation:
        query = {}
    else:
        query = {
            '$or': [
                {'liquidity': {'$ne': 0}},
                {'points': {'$exists': False}}
            ]
        }
    await filter_by_the_latest_value(query)
    for record in db[Collection.POSITIONS].find(query):
        yield record


async def handle_lp_leaderboard(db: Database, rpc_url: str):
    processed_positions_records = 0
    positions_update_operations = []
    async for position_record in yield_position_records(db):
        simulated_tx_fees_usd = 0
        collected_fees_usd = 0

        if Decimal(position_record.get('liquidity', 0)) > ZERO_DECIMAL:
            token0_fees, token1_fees = await simulate_collect_tx(rpc_url, position_record)

            if token0_fees and token1_fees:
                token0_fees = await amount_after_decimals(token0_fees, position_record['token0Decimals'])
                token1_fees = await amount_after_decimals(token1_fees, position_record['token1Decimals'])

                hour_id, _ = await get_hour_id(position_record['timestamp'])
                token0_price = await get_token_price_by_hour_id(db, hour_id, position_record['token0Address'])
                token1_price = await get_token_price_by_hour_id(db, hour_id, position_record['token1Address'])
                token0_fees_usd = token0_fees * token0_price
                token1_fees_usd = token1_fees * token1_price

                simulated_tx_fees_usd = token0_fees_usd + token1_fees_usd

        position_fee_record = await get_position_fee_by_position_id(db, position_record['positionId'])
        if position_fee_record:
            token0_fees = await amount_after_decimals(Decimal(position_fee_record['collectedFeesToken0']),
                                                      position_record['token0Decimals'])
            token1_fees = await amount_after_decimals(Decimal(position_fee_record['collectedFeesToken1']),
                                                      position_record['token1Decimals'])

            hour_id, _ = await get_hour_id(position_fee_record['timestamp'])
            token0_price = await get_token_price_by_hour_id(db, hour_id, position_fee_record['token0Address'])
            token1_price = await get_token_price_by_hour_id(db, hour_id, position_fee_record['token1Address'])
            token0_fees_usd = token0_fees * token0_price
            token1_fees_usd = token1_fees * token1_price

            collected_fees_usd = token0_fees_usd + token1_fees_usd

        day_id, _ = await get_day_id(position_record['timestamp'])
        pool = await get_pool_by_tokens(db, position_record['token0Address'], position_record['token1Address'],
                                        position_record['poolFee'])
        pool_current_volume = await get_pool_volume_by_day_id(db, day_id, pool['poolAddress'])
        pool_previous_day_volume = await get_pool_volume_by_day_id(db, day_id - 1, pool['poolAddress'])

        current_lp_streak = position_record.get('currentLpStreak', 0)
        total_fees_usd = position_record.get('totalFeesUSD', ZERO_DECIMAL128).to_decimal()
        previous_day_fees_usd = position_record.get('previousDayFeesUSD', ZERO_DECIMAL128).to_decimal()

        current_fees_usd = simulated_tx_fees_usd + collected_fees_usd - total_fees_usd
        if current_fees_usd < ZERO_DECIMAL:
            current_fees_usd = ZERO_DECIMAL

        if pool_current_volume > ZERO_DECIMAL and (
                previous_day_fees_usd > LP_CONST * current_fees_usd * (
                pool_previous_day_volume / pool_current_volume)
        ):
            current_lp_streak += 1
        else:
            current_lp_streak = 1

        position_update_data = {
            'totalFeesUSD': Decimal128(current_fees_usd + total_fees_usd),
            'previousDayFeesUSD': Decimal128(current_fees_usd),
            'currentLpStreak': current_lp_streak,
        }

        positions_update_operations.append(
            UpdateOne(
                {'_id': position_record['_id']},
                {'$set': position_update_data},
            ))
        processed_positions_records += 1

    if positions_update_operations:
        db[Collection.POSITIONS].bulk_write(positions_update_operations)
    logger.info(f'Successfully processed {processed_positions_records} Positions records for leaderboard contest')


async def calculate_lp_leaderboard(db: Database):
    users_result = defaultdict(int)
    async for position_record in yield_position_records(db, points_calculation=True):
        current_lp_streak = position_record.get('currentLpStreak', 0)
        total_fees_usd = position_record.get('totalFeesUSD', ZERO_DECIMAL128).to_decimal()
        owner_address = position_record['ownerAddress']

        pool_boost = get_pool_boost(position_record['token0Address'], position_record['token1Address'])
        streak_boost = get_streak_boost(current_lp_streak)

        points = total_fees_usd * pool_boost * streak_boost * Decimal(1000)
        users_result[owner_address] += points

    current_timestamp = int(time.time() * 1000)
    day_id, _ = await get_day_id(current_timestamp)

    processed_positions_records = 0
    lp_contest_update_operations = []
    lp_contest_snapshot_update_operations = []
    for user, points in users_result.items():
        lp_contest_update_operations.append(
            UpdateOne(
                {'userAddress': user},
                {'$set': {
                    'points': Decimal128(points),
                }},
                upsert=True
            ))
        lp_contest_snapshot_update_operations.append(
            UpdateOne(
                {
                    'userAddress': user,
                    'dayId': day_id,
                },
                {'$set': {
                    'points': Decimal128(points),
                }},
                upsert=True
            ))
        processed_positions_records += 1

    if lp_contest_update_operations:
        db[Collection.LP_LEADERBOARD].bulk_write(lp_contest_update_operations)
    if lp_contest_snapshot_update_operations:
        db[Collection.LP_LEADERBOARD_SNAPSHOT].bulk_write(lp_contest_snapshot_update_operations)
    logger.info(f'Successfully calculated {processed_positions_records} Positions records for leaderboard contest')


async def process_leaderboard(mongo_url: str, mongo_database: Database, rpc_url: str):
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        await handle_lp_leaderboard(db, rpc_url)
        await calculate_lp_leaderboard(db)


async def run_leaderboard_transformer(mongo_url: str, mongo_database: Database, rpc_url: str):
    while True:
        await process_leaderboard(mongo_url, mongo_database, rpc_url)
        time.sleep(DAY_INTERVAL)
