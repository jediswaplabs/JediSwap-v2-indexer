from decimal import Decimal
import os

from bson import Decimal128
from pymongo.database import Database

from server.const import Collection, Event, ZERO_DECIMAL, ZERO_DECIMAL128, MAX_UINT128, ETH, USDC, USDT, STRK
from server.query_utils import simulate_tx, get_position_record
from server.utils import get_hour_id, format_address

from starknet_py.contract import ContractFunction
from starknet_py.net.client_models import TransactionType
from starknet_py.net.full_node_client import FullNodeClient
from structlog import get_logger


logger = get_logger(__name__)


TIME_VESTED_CONST = 1296000  # 15 days


async def get_pool_boost(token0_address: str, token1_address) -> Decimal:
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


async def get_current_position_total_fees_usd(event_data: dict, position_record: dict, db: Database, rpc_url: str) -> Decimal:
    simulated_tx_fees_usd = 0
    hour_id, _ = await get_hour_id(event_data['timestamp'])
    token0_price = await get_token_price_by_hour_id(db, hour_id, position_record['token0Address'])
    token1_price = await get_token_price_by_hour_id(db, hour_id, position_record['token1Address'])
    
    token0_fees, token1_fees = await simulate_collect_tx(rpc_url, position_record, event_data['block'])
    if token0_fees or token1_fees:
        token0_fees_usd = token0_fees * token0_price
        token1_fees_usd = token1_fees * token1_price
        simulated_tx_fees_usd = token0_fees_usd + token1_fees_usd

    token0_fees_usd = position_record['collectedFeesToken0'].to_decimal() * token0_price
    token1_fees_usd = position_record['collectedFeesToken1'].to_decimal() * token1_price
    collected_fees_usd = token0_fees_usd + token1_fees_usd

    return collected_fees_usd + simulated_tx_fees_usd


async def get_time_vested_value(event: str | None, event_data: dict, position_record: dict) -> tuple[Decimal, Decimal]:
    time_vested_value = position_record['timeVestedValue'].to_decimal()
    period = event_data['timestamp'] - position_record['lastUpdatedTimestamp']
    time_vested_value = min(Decimal(1), time_vested_value + Decimal(period / TIME_VESTED_CONST))
    if event == Event.DECREASE_LIQUIDITY:
        new_time_vested_value = ZERO_DECIMAL
    elif event == Event.INCREASE_LIQUIDITY:
        current_liquidity = Decimal(position_record['liquidity'])
        new_liquidity = Decimal(event_data['liquidity'])
        if not current_liquidity:
            current_liquidity = new_liquidity
        new_time_vested_value = time_vested_value * Decimal(1) / (new_liquidity / current_liquidity)
    else:
        new_time_vested_value = time_vested_value

    return time_vested_value, new_time_vested_value


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


async def simulate_collect_tx(rpc_url: str, position_record: dict, block_number: int) -> tuple[Decimal, Decimal]:
    rpc = FullNodeClient(node_url=rpc_url)
    nonce = await rpc.get_contract_nonce(format_address(position_record['ownerAddress']), block_number=block_number)

    for nft_contract in os.environ['NFT_ROUTER_CONTRACTS'].split(','):
        for is_braavos_account in [False, True]:
            collect_tx = CollectTx(position_record, nft_contract, nonce, is_braavos_account)
            result = await simulate_tx(collect_tx, rpc_url, block_number)
            if result:
                return result[-2], result[-1]
    else:
        logger.warn(f"Couldn't fetch fees from position: {position_record['positionId']}")
        return ZERO_DECIMAL, ZERO_DECIMAL


async def get_token_price_by_hour_id(db: Database, hour_id: int, token_address: str) -> Decimal:
    query = {
        'hourId': hour_id,
        'tokenAddress': token_address,
    }
    token_hour_data = db[Collection.TOKENS_HOUR_DATA].find_one(query)
    if not token_hour_data:
        logger.info(f'Token price for {token_address} and hour id {hour_id} not found')
        token_hour_data = {}
    return token_hour_data.get('priceUSD', ZERO_DECIMAL128).to_decimal()


async def calculate_lp_leaderboard_points(event_data: dict, db: Database, rpc_url: str, event: str | None = None):
    position_record = await get_position_record(db, event_data['positionId'])

    current_position_total_fees_usd = await get_current_position_total_fees_usd(event_data, position_record, db, rpc_url)
    total_fees_usd = position_record['totalFeesUSD'].to_decimal()
    current_fees_usd = current_position_total_fees_usd - total_fees_usd
    if current_fees_usd < ZERO_DECIMAL:
        current_fees_usd = ZERO_DECIMAL

    time_vested_value, new_time_vested_value = await get_time_vested_value(event, event_data, position_record)

    pool_boost = await get_pool_boost(position_record['token0Address'], position_record['token1Address'])

    points = current_fees_usd * time_vested_value * pool_boost * Decimal(1000)

    position_update_data = dict()
    position_update_data['$set'] = dict()
    position_update_data['$inc'] = dict()
    position_update_data['$set']['totalFeesUSD'] = Decimal128(current_position_total_fees_usd)
    position_update_data['$set']['timeVestedValue'] = Decimal128(new_time_vested_value)
    position_update_data['$set']['lastUpdatedTimestamp'] = event_data['timestamp']
    position_update_data['$inc']['lp_points'] = Decimal128(points)

    position_query = {'positionId': position_record['positionId']}
    db[Collection.POSITIONS].update_one(position_query, position_update_data)

    lp_leaderboard_snapshot_record = {
        'positionId': position_record['positionId'],
        'currentFeesUsd': Decimal128(current_fees_usd),
        'timeVestedValue': Decimal128(time_vested_value),
        'lastUpdatedTimestamp': event_data['timestamp'],
        'lp_points': Decimal128(points),
    }
    db[Collection.LP_LEADERBOARD_SNAPSHOT].insert_one(lp_leaderboard_snapshot_record)
