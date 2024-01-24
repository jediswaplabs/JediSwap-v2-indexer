from decimal import Decimal

from pymongo.database import Database
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient

from server.const import ETH_USDC_ADDRESS, STABLECOINS, WHITELISTED_POOLS, ETH, WHITELISTED_TOKENS, ZERO_DECIMAL128
from server.query_utils import get_pool, get_tokens_from_pool
from server.utils import exponent_to_decimal, safe_div


Q192 = Decimal(2 ** 192)
MINIMUM_ETH_LOCKED = Decimal(60)


class EthPrice:
    _ETH_PRICE = Decimal(0)

    @classmethod
    def get(cls) -> Decimal:
        return cls._ETH_PRICE

    @classmethod
    def set(cls, rpc_url: str):
        cls._ETH_PRICE = cls.get_eth_price(rpc_url)
    
    @staticmethod
    def get_eth_price(rpc_url: str) -> Decimal:
        contract = Contract.from_address_sync(
            address=ETH_USDC_ADDRESS,
            provider=FullNodeClient(node_url=rpc_url),
        )
        if contract is not None:
            (value,) = contract.functions["get_token1"].call_sync()
            return Decimal(value)
        return Decimal(0)


def find_eth_per_token(db: Database, token_addr: str) -> Decimal:
    if token_addr == ETH:
        return Decimal(1)
      
    largest_liquidity_eth = Decimal(0)
    price_so_far = Decimal(0)

    if token_addr in STABLECOINS:
        price_so_far = safe_div(Decimal(1), EthPrice.get())
    else:
        for pool_address in WHITELISTED_POOLS:
            pool = get_pool(db, pool_address)
            if pool and pool['liquidity'].to_decimal() > Decimal(0):
                token0, token1 = get_tokens_from_pool(db, pool)
                if pool['token0'] == token_addr:
                    eth_locked = pool['totalValueLockedToken1'].to_decimal() * token1['derivedETH'].to_decimal()
                    if eth_locked > largest_liquidity_eth & eth_locked > MINIMUM_ETH_LOCKED:
                        largest_liquidity_eth = eth_locked
                        price_so_far = pool['token1Price'].to_decimal() * token1['derivedETH'].to_decimal()
                if pool['token1'] == token_addr:
                    eth_locked = pool['totalValueLockedToken0'].to_decimal() * token0['derivedETH'].to_decimal()
                    if eth_locked > largest_liquidity_eth & eth_locked > MINIMUM_ETH_LOCKED:
                        largest_liquidity_eth = eth_locked
                        price_so_far = pool['token0Price'].to_decimal() * token0['derivedETH'].to_decimal()
    return price_so_far

def get_tracked_amount_usd(amount0_abs: Decimal, token0_address: str, token0_derivedETH: Decimal, amount1_abs: Decimal, token1_address: str, token1_derivedETH: Decimal) -> Decimal:
    price0_USD = token0_derivedETH * EthPrice.get()
    price1_USD = token1_derivedETH * EthPrice.get()

    if (token0_address in WHITELISTED_TOKENS and token1_address in WHITELISTED_TOKENS):
        return (amount0_abs * price0_USD + amount1_abs * price1_USD)
    
    if (token0_address in WHITELISTED_TOKENS and token1_address not in WHITELISTED_TOKENS):
        return amount0_abs * price0_USD * 2
    
    if (token0_address not in WHITELISTED_TOKENS and token1_address in WHITELISTED_TOKENS):
        return amount1_abs * price1_USD * 2
    
    return ZERO_DECIMAL128


def sqrt_price_x96_to_token_prices(sqrt_price_x96: float, token0_decimals: int, token1_decimals: int
                                   ) -> tuple[Decimal, Decimal]:
    num = Decimal(sqrt_price_x96) ** 2
    price1 = num / Q192 * exponent_to_decimal(token0_decimals) / exponent_to_decimal(token1_decimals)
    price0 = safe_div(Decimal('1'), price1)
    return price0, price1

