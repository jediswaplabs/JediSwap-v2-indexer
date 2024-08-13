from decimal import Decimal

from pymongo.database import Database

from server.const import ETH_USDC_ADDRESS, STABLECOINS, ETH, WHITELISTED_TOKENS, ZERO_DECIMAL
from server.query_utils import get_pool_record, get_tokens_from_pool, get_all_token_pools
from server.utils import exponent_to_decimal, safe_div

from structlog import get_logger

logger = get_logger(__name__)


Q192 = Decimal(2 ** 192)
MINIMUM_ETH_LOCKED = Decimal(0)


class EthPrice:
    _ETH_PRICE = Decimal(0)

    @classmethod
    async def get(cls) -> Decimal:
        return cls._ETH_PRICE

    @classmethod
    async def set(cls, db: Database):
        cls._ETH_PRICE = await cls.get_eth_price(db)
    
    @staticmethod
    async def get_eth_price(db: Database) -> Decimal:
        pool = await get_pool_record(db, ETH_USDC_ADDRESS)
        if pool and 'token0Price' in pool.keys() and pool['token0Price'].to_decimal() != Decimal(0) and 'token1Price' in pool.keys() and pool['token1Price'].to_decimal() != Decimal(0):
            if (pool['token0'] == ETH):
                return pool['token1Price'].to_decimal()
            else:
                return pool['token0Price'].to_decimal()
        else:
            return Decimal(2500)


async def find_eth_per_token(db: Database, token_addr: str, rpc_url: str) -> Decimal:
    if token_addr == ETH:
        return Decimal(1)
      
    largest_liquidity_eth = ZERO_DECIMAL
    price_so_far = ZERO_DECIMAL

    if token_addr in STABLECOINS:
        eth_price = await EthPrice.get()
        price_so_far = await safe_div(Decimal(1), eth_price)
    else:
        for pool in await get_all_token_pools(db, token_addr):
            pool = await get_pool_record(db, pool['poolAddress'])
            if pool.get('liquidity') and pool.get('liquidity').to_decimal() > 0:
                token0, token1 = await get_tokens_from_pool(db, pool, rpc_url)
                if pool['token0'] == token_addr:
                    if price_so_far == ZERO_DECIMAL:
                        price_so_far = pool['token1Price'].to_decimal() * token1['derivedETH'].to_decimal()
                    else:
                        eth_locked = pool['totalValueLockedToken1'].to_decimal() * token1['derivedETH'].to_decimal()
                        if eth_locked > largest_liquidity_eth and eth_locked > MINIMUM_ETH_LOCKED:
                            largest_liquidity_eth = eth_locked
                            price_so_far = pool['token1Price'].to_decimal() * token1['derivedETH'].to_decimal()
                elif pool['token1'] == token_addr:
                    if price_so_far == ZERO_DECIMAL:
                        price_so_far = pool['token0Price'].to_decimal() * token0['derivedETH'].to_decimal()
                    else:
                        eth_locked = pool['totalValueLockedToken0'].to_decimal() * token0['derivedETH'].to_decimal()
                        if eth_locked > largest_liquidity_eth and eth_locked > MINIMUM_ETH_LOCKED:
                            largest_liquidity_eth = eth_locked
                            price_so_far = pool['token0Price'].to_decimal() * token0['derivedETH'].to_decimal()
    return price_so_far


async def get_tracked_amount_usd(amount0_abs: Decimal, token0_address: str, token0_derivedETH: Decimal, amount1_abs: Decimal, token1_address: str, token1_derivedETH: Decimal) -> Decimal:
    eth_price = await EthPrice.get()
    price0_USD = token0_derivedETH * eth_price
    price1_USD = token1_derivedETH * eth_price

    if (token0_address in WHITELISTED_TOKENS and token1_address in WHITELISTED_TOKENS):
        return (amount0_abs * price0_USD + amount1_abs * price1_USD)
    
    if (token0_address in WHITELISTED_TOKENS and token1_address not in WHITELISTED_TOKENS):
        return amount0_abs * price0_USD * 2
    
    if (token0_address not in WHITELISTED_TOKENS and token1_address in WHITELISTED_TOKENS):
        return amount1_abs * price1_USD * 2
    
    return ZERO_DECIMAL


async def sqrt_price_x96_to_token_prices(sqrt_price_x96: float, token0_decimals: int, token1_decimals: int
                                   ) -> tuple[Decimal, Decimal]:
    num = Decimal(sqrt_price_x96) ** 2
    token0_decimals_ = await exponent_to_decimal(token0_decimals)
    token1_decimals_ = await exponent_to_decimal(token1_decimals)
    price1 = num / Q192 * token0_decimals_ / token1_decimals_
    price0 = await safe_div(Decimal('1'), price1)
    return price0, price1

