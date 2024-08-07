from bson import Decimal128
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from pymongo.database import Database

from server.const import (
    Collection, Event, ZERO_DECIMAL, ZERO_DECIMAL128, MAX_UINT128, ETH,
    USDC, USDT, STRK, DEFAULT_DECIMALS
)
from server.graphql.resolvers.helpers import convert_timestamp_to_datetime
from server.query_utils import simulate_tx, get_position_record, get_token_record
from server.utils import get_hour_id, format_address, amount_after_decimals

from starknet_py.contract import ContractFunction
from starknet_py.net.client_models import TransactionType
from starknet_py.net.full_node_client import FullNodeClient
from structlog import get_logger


logger = get_logger(__name__)


TIME_VESTED_CONST = 1296000000  # 15 days


async def get_pool_boost(token0_address: str, token1_address: str) -> Decimal:
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


async def get_current_position_total_fees_usd(event_data: dict, position_record: dict, snapshot_record: dict, db: Database, rpc_url: str, 
                                              get_uncollected_fees_func: callable) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
    current_unclaimed_token0_fees = ZERO_DECIMAL
    current_unclaimed_token1_fees = ZERO_DECIMAL

    hour_id, _ = await get_hour_id(event_data['timestamp'])
    token0_price = await get_token_price_by_hour_id(db, hour_id, position_record['token0Address'])
    token1_price = await get_token_price_by_hour_id(db, hour_id, position_record['token1Address'])
    
    token0_fees, token1_fees = await get_uncollected_fees_func(rpc_url, position_record, event_data['block'])
    if token0_fees or token1_fees:
        token0 = await get_token_record(db, position_record['token0Address'], rpc_url)
        token1 = await get_token_record(db, position_record['token1Address'], rpc_url)
        current_unclaimed_token0_fees = await amount_after_decimals(token0_fees, token0.get('decimals', DEFAULT_DECIMALS))
        current_unclaimed_token1_fees = await amount_after_decimals(token1_fees, token1.get('decimals', DEFAULT_DECIMALS))

    current_token0_fees = snapshot_record['collectedFeesToken0'].to_decimal() - position_record['lastUnclaimedFeesToken0'].to_decimal() + current_unclaimed_token0_fees
    current_token0_fees_usd = current_token0_fees * token0_price
    current_token1_fees = snapshot_record['collectedFeesToken1'].to_decimal() - position_record['lastUnclaimedFeesToken1'].to_decimal() + current_unclaimed_token1_fees
    current_token1_fees_usd = current_token1_fees * token1_price
    current_fees_usd = current_token0_fees_usd + current_token1_fees_usd

    return current_fees_usd, current_unclaimed_token0_fees, current_unclaimed_token1_fees, token0_price, token1_price


async def get_time_vested_value(record: dict, position_record_in_event: dict, latest_position_record: dict
                                ) -> tuple[Decimal, Decimal, float]:
    last_time_vested_value = latest_position_record['timeVestedValue'].to_decimal()
    period = record['timestamp'] - latest_position_record['lastUpdatedTimestamp']
    last_time_vested_value = min(Decimal(1), last_time_vested_value + Decimal(period / TIME_VESTED_CONST))

    if record['event'] == Event.DECREASE_LIQUIDITY:
        current_time_vested_value = ZERO_DECIMAL
    elif record['event'] == Event.INCREASE_LIQUIDITY:
        current_liquidity = position_record_in_event['liquidity'].to_decimal()
        new_liquidity = current_liquidity + record['liquidity'].to_decimal()
        if not current_liquidity:
            current_liquidity = new_liquidity
        current_time_vested_value = last_time_vested_value * Decimal(1) / (new_liquidity / current_liquidity)
    else:
        current_time_vested_value = last_time_vested_value

    return last_time_vested_value, current_time_vested_value, period


class CollectTx:
    METHOD = 'collect'
    TOTAL_TX = '0x1'
    COLLECT_OFFSET = '0x0'
    COLLECT_CALL_DATA_LENGTH = 5
    TOTAL_CALL_DATA_LENGTH = '0x5'

    def __init__(self, position_record: dict, nonce: int, braavos_account: bool = False) -> None:
        contract_address = format_address(position_record['positionAddress'])
        recipient = format_address(position_record['ownerAddress'])
        method_calldata = [
            position_record['positionId'],
            0,  # position_id is uint256
            recipient,
            MAX_UINT128,
            MAX_UINT128,
        ]

        if braavos_account:
            calldata = [
                self.TOTAL_TX,
                contract_address,
                ContractFunction.get_selector(self.METHOD),
                self.COLLECT_OFFSET,
                self.COLLECT_CALL_DATA_LENGTH,
                self.TOTAL_CALL_DATA_LENGTH
            ]
        else:
            calldata = [
                self.TOTAL_TX,
                contract_address,
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


async def simulate_collect_tx(rpc_url: str, position_record: dict, block_number: int) -> tuple[Decimal, Decimal]:
    rpc = FullNodeClient(node_url=rpc_url)
    try:
        nonce = await rpc.get_contract_nonce(format_address(position_record['ownerAddress']), block_number=block_number)
    except Exception:
        logger.warning(f"Cannot get a nonce for position {position_record['positionId']}. Retrying...")
        nonce = await rpc.get_contract_nonce(format_address(position_record['ownerAddress']), block_number=block_number)
        logger.info('Sucessfully retrieved nonce')

    tx_errors = []
    for is_braavos_account in [False, True]:
        collect_tx = CollectTx(position_record, nonce, is_braavos_account)
        try:
            result = await simulate_tx(collect_tx, rpc_url, block_number)
            if result:
                return result[-2], result[-1]
        except Exception as exc:
            tx_errors.append(exc)
        
    if tx_errors:
        logger.warn(f"Couldn't fetch fees from position {position_record['positionId']}:")
        for error in tx_errors:
            logger.warn(error)
    return ZERO_DECIMAL, ZERO_DECIMAL


async def get_token_price_by_hour_id(db: Database, hour_id: int, token_address: str) -> Decimal:
    query = {
        'hourId': {
            '$lte': hour_id,
        },
        'priceUSD': {
            '$ne': 0
        },
        'tokenAddress': token_address,
    }
    token_hour_data = list(db[Collection.TOKENS_HOUR_DATA].find(query).sort({'hourId': -1}))
    if not token_hour_data:
        logger.info(f'Token price for {token_address} and hour id {hour_id} not found')
        return ZERO_DECIMAL
    return token_hour_data[0]['priceUSD'].to_decimal()


async def insert_lp_snapshot_to_db(position_id: str, event_data: dict, db: Database, event: str | None = None, 
                                   position_record: dict | None = None, amount0_fees: Decimal = ZERO_DECIMAL, 
                                   amount1_fees: Decimal = ZERO_DECIMAL):
    lp_leaderboard_snapshot_record = {
        'positionId': position_id,
        'position': position_record,
        'liquidity': Decimal128(event_data.get('liquidity', '0')),
        'timestamp': event_data['timestamp'],
        'block': event_data['block'],
        'event': event,
        'currentFeesUsd': ZERO_DECIMAL128,
        'lpPoints': ZERO_DECIMAL128,
        'lastTimeVestedValue': ZERO_DECIMAL128,
        'currentTimeVestedValue': ZERO_DECIMAL128,
        'period': 0,
        'poolBoost': ZERO_DECIMAL128,
        'processed': False,
        'collectedFeesToken0': Decimal128(amount0_fees),
        'collectedFeesToken1': Decimal128(amount1_fees),
        'lastUnclaimedFeesToken0': ZERO_DECIMAL128,
        'lastUnclaimedFeesToken1': ZERO_DECIMAL128,
        'currentUnclaimedFeesToken0': ZERO_DECIMAL128,
        'currentUnclaimedFeesToken1': ZERO_DECIMAL128,
        'token0Price': ZERO_DECIMAL128,
        'token1Price': ZERO_DECIMAL128,
    }
    db[Collection.LP_LEADERBOARD_SNAPSHOT].insert_one(lp_leaderboard_snapshot_record)


async def insert_lp_leaderboard_snapshot(event_data: dict, db: Database, event: str | None = None, 
                                         position_record: dict | None = None, teahouse: bool = False):
    if not position_record:
        position_record = await get_position_record(db, event_data['positionId'])

    position_id = position_record.get('positionId', '')
    lp_snapshot_query = {
        'positionId': position_id,
        'timestamp': event_data['timestamp'],
        'event': event,
    }
    if teahouse:
        lp_snapshot_query['position.vaultAddress'] = position_record['vaultAddress']
        lp_snapshot_query['position.poolAddress'] = position_record['poolAddress']

    record = db[Collection.LP_LEADERBOARD_SNAPSHOT].find_one(lp_snapshot_query)
    if not record:
        await insert_lp_snapshot_to_db(position_id, event_data, db, event, position_record)


async def update_lp_leaderboard_snapshot_decrease_liquidity_event(
        event_data: dict, db: Database, position_record: dict | None = None, teahouse: bool = False,
        amount0_fees: Decimal = ZERO_DECIMAL, amount1_fees: Decimal = ZERO_DECIMAL) -> bool:
    lp_snapshot_query = {
        'positionId': position_record.get('positionId', ''),
        'block': event_data['block'],
        'event': Event.DECREASE_LIQUIDITY,
    }
    if teahouse:
        lp_snapshot_query['position.vaultAddress'] = position_record['vaultAddress']
        lp_snapshot_query['position.poolAddress'] = position_record['poolAddress']

    if record := db[Collection.LP_LEADERBOARD_SNAPSHOT].find_one(lp_snapshot_query):
        record_query = {'_id': record['_id']}
        db[Collection.LP_LEADERBOARD_SNAPSHOT].update_one(record_query, {
            '$inc': {
                'collectedFeesToken0': Decimal128(amount0_fees),
                'collectedFeesToken1': Decimal128(amount1_fees),
            }
        })
        return True
    return False
        

async def insert_lp_leaderboard_snapshot_collect_event(
        event_data: dict, db: Database, position_record: dict | None = None, teahouse: bool = False,
        amount0_fees: Decimal = ZERO_DECIMAL, amount1_fees: Decimal = ZERO_DECIMAL):
    current_dt = convert_timestamp_to_datetime(event_data['timestamp'])
    current_dt = current_dt.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=timezone.utc)
    current_dt = current_dt - timedelta(days=1)
    timestamp1 = int(current_dt.timestamp() * 1000)

    current_dt = convert_timestamp_to_datetime(event_data['timestamp'])
    current_dt = current_dt.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=timezone.utc)
    timestamp2 = int(current_dt.timestamp() * 1000)

    for timestamp in [timestamp1, timestamp2]:
        event_data = event_data.copy()
        event_data['timestamp'] = timestamp

        position_id = position_record.get('positionId', '')
        lp_snapshot_query = {
            'positionId': position_id,
            'timestamp': event_data['timestamp'],
        }
        if teahouse:
            lp_snapshot_query['position.vaultAddress'] = position_record['vaultAddress']
            lp_snapshot_query['position.poolAddress'] = position_record['poolAddress']

        if record := db[Collection.LP_LEADERBOARD_SNAPSHOT].find_one(lp_snapshot_query):
            record_query = {'_id': record['_id']}
            db[Collection.LP_LEADERBOARD_SNAPSHOT].update_one(record_query, {
                '$inc': {
                    'collectedFeesToken0': Decimal128(amount0_fees),
                    'collectedFeesToken1': Decimal128(amount1_fees),
                }
            })
            return

    # if the lp snapshot records doesn't exist for timestamps create it
    if position_record.get('liquidity', ZERO_DECIMAL128).to_decimal() > ZERO_DECIMAL:
        event_data = event_data.copy()
        event_data['timestamp'] = timestamp2
        await insert_lp_snapshot_to_db(position_id, event_data, db, position_record=position_record,
                                       amount0_fees=amount0_fees, amount1_fees=amount1_fees)
    else:
        event_data = event_data.copy()
        event_data['timestamp'] = timestamp1
        await insert_lp_snapshot_to_db(position_id, event_data, db, position_record=position_record,
                                       amount0_fees=amount0_fees, amount1_fees=amount1_fees)



async def get_closest_block_from_timestamp(db: Database, target_timestamp: float) -> int | None:
    pipeline = [
        {
            '$addFields': {
                'diff': {
                    '$abs': {
                        '$subtract': ['$timestamp', target_timestamp]
                    }
                }
            }
        },
        {
            '$sort': {
                'diff': 1
            }
        },
        {
            '$limit': 1
        }
    ]
    result = list(db[Collection.BLOCKS].aggregate(pipeline))
    return result[0] if result else None


async def is_lp_leaderboard_record_processed(db: Database, dt_obj: datetime, position_record: dict,
                                             records_to_be_inserted: list) -> bool:
    timestamp = int(dt_obj.timestamp() * 1000)
    block = await get_closest_block_from_timestamp(db, timestamp)
    if not block:
        logger.warning(f'Cannot get a block for position {position_record["positionId"]} '
                       f'and timestamp {timestamp}. Skipping the position')
        return False
    event_data = {
        'positionId': position_record.get('positionId'),
        'timestamp': timestamp,
        'block': int(block['blockNumber']),
    }
    records_to_be_inserted.append({
        'event_data': event_data,
        'position_record': position_record,
    })
    return True


async def process_position_for_lp_leaderboard(db: Database, current_dt: datetime, last_updated_dt: datetime, 
                                              position_record: dict) -> tuple[bool, list]:
    records_to_be_inserted = []
    missing_block = False
    while current_dt >= last_updated_dt:
        if not await is_lp_leaderboard_record_processed(db, last_updated_dt, position_record, records_to_be_inserted):
            missing_block = True
            return missing_block, []
        last_updated_dt = last_updated_dt + timedelta(days=1)

    return missing_block, records_to_be_inserted


async def process_position_for_lp_leaderboard_for_position_transformer(db: Database, record: dict, position_record: dict
                                                                       ) -> tuple[bool, list]:
    last_updated_dt = convert_timestamp_to_datetime(position_record['lastUpdatedTimestamp'])
    last_updated_dt = last_updated_dt.replace(tzinfo=timezone.utc)

    current_dt = convert_timestamp_to_datetime(record['timestamp'])
    current_dt = current_dt.replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=timezone.utc)
    current_dt = current_dt - timedelta(days=1)

    records_to_be_inserted = []
    missing_block = False
    while current_dt >= last_updated_dt:
        if not await is_lp_leaderboard_record_processed(db, current_dt, position_record, records_to_be_inserted):
            missing_block = True
            return missing_block, []
        current_dt = current_dt - timedelta(days=1)

    return missing_block, records_to_be_inserted
