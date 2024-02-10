from decimal import Decimal
import os

from bson import Decimal128
from server.utils import format_address


class Collection:
    POOLS = 'pools'
    POOLS_DATA = 'pools_data'
    POSITIONS = 'positions'
    POSITION_FEES = 'position_fees'
    TOKENS = 'tokens'
    FACTORIES = 'factories'
    FACTORIES_DAY_DATA = 'factories_day_data'
    POOLS_DAY_DATA = 'pools_day_data'
    POOLS_HOUR_DATA = 'pools_hour_data'
    TOKENS_DAY_DATA = 'tokens_day_data'
    TOKENS_HOUR_DATA = 'tokens_hour_data'

FACTORY_ADDRESS = format_address(os.environ.get('FACTORY_CONTRACT'))
NFT_ROUTER = format_address(os.environ.get('NFT_ROUTER_CONTRACT'))


# mainnet contracts
if os.environ.get('NETWORK') == 'mainnet':
    ETH = '0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7'
    USDC = '0x53c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8'
    DAI = '0xda114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3'
    USDT = '0x68f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8'
    WBTC = '0x3fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac'
    WSTETH = "0x42b8f0484674ca266ac5d08e4ac6a3fe65bd3129795def2dca5c34ecc5f96d2"
    LORDS = "0x124aeb495b947201f5fac96fd1138e326ad86195b98df6dec9009158a533b49"
    LUSD = "0x70a76fd48ca0ef910631754d77dd822147fe98a569b826ec85e3c33fde586ac"

    ETH_USDC_ADDRESS = '0x7015a6822f109a2e41d25dd6878fe161ae9bb13eeb87e62de42a3158a64db28'
    WHITELISTED_POOLS = []  # todo
    STABLECOINS = [USDC, USDT, DAI, LUSD]
    WHITELISTED_TOKENS = [ETH, USDC, DAI, USDT, WBTC, WSTETH, LORDS, LUSD]
else:
    ETH = '0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7'
    USDC = '0x5a643907b9a4bc6a55e9069c4fd5fd1f5c79a22470690f75556c4736e34426'
    DAI = '0x3e85bfbb8e2a42b7bead9e88e9a1b19dbccf661471061807292120462396ec9'
    USDT = '0x6a8f0e0d3bf9a6f049ce3fcbad7e26cbd0ceb3f0bfc6160f4ab7bdd7985b0e3' # not sure
    WBTC = '0x12d537dc323c439dc65c976fad242d5610d27cfb5f31689a0a319b8be7f3d56'

    ETH_USDC_ADDRESS = '0xcc2f78f58cd3d242fc7b0661452a7e275f701516459a46b3792616fc270bed'

    WHITELISTED_POOLS = [
        '0x15b5061cab98c4b3a7d26cba1ae33636337c5bc8e17ba4cd9dcc5c9bab2c615',
    ]
    STABLECOINS = [USDC, USDT, DAI]
    WHITELISTED_TOKENS = [ETH, USDC, DAI, USDT, WBTC]

ZERO_ADDRESS = '0x0000000000000000000000000000000000000000000000000000000000000000'
ZERO_DECIMAL128 = Decimal128(Decimal(0))
ZERO_DECIMAL = Decimal(0)

DEFAULT_DECIMALS = 18
TIME_INTERVAL = 60  # in seconds
