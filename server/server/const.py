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
    FACTORIES_HOUR_DATA = 'factories_hour_data'
    POOLS_DAY_DATA = 'pools_day_data'
    POOLS_HOUR_DATA = 'pools_hour_data'
    TOKENS_DAY_DATA = 'tokens_day_data'
    TOKENS_HOUR_DATA = 'tokens_hour_data'


FACTORY_ADDRESS = format_address(str(os.environ.get('FACTORY_CONTRACT')))


# mainnet contracts
if os.environ.get('NETWORK') == 'mainnet':
    ETH = '0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7'
    USDC = '0x53c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8'
    DAI = '0x05574eb6b8789a91466f902c380d978e472db68170ff82a5b650b95a58ddf4ad'
    USDT = '0x68f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8'
    WBTC = '0x3fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac'
    WSTETH = '0x42b8f0484674ca266ac5d08e4ac6a3fe65bd3129795def2dca5c34ecc5f96d2'
    LORDS = '0x124aeb495b947201f5fac96fd1138e326ad86195b98df6dec9009158a533b49'
    LUSD = '0x70a76fd48ca0ef910631754d77dd822147fe98a569b826ec85e3c33fde586ac'
    STRK = '0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d'

    ETH_USDC_ADDRESS = '0x7015a6822f109a2e41d25dd6878fe161ae9bb13eeb87e62de42a3158a64db28'
    WHITELISTED_POOLS = []  # todo
    STABLECOINS = [USDC, USDT, DAI, LUSD]
    WHITELISTED_TOKENS = [ETH, USDC, DAI, USDT, WBTC, WSTETH, LORDS, LUSD, STRK]
else:
    ETH = '0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7'
    USDC = '0x03a909c1f2d1900d0c96626fac1bedf1e82b92110e5c529b05f9138951b93535'
    DAI = '0x01f3b27e2f13d7d86f7f4c7dceb267290f158ac383803b22b712f7f9e58905ef'
    USDT = '0x07d83b422a5fee99afaca50b6adf7de759af4a725f61cce747e06b6c09f7ab38'
    WBTC = '0x00c6164da852d230360333d6ade3551ee3e48124c815704f51fa7f12d8287dcc'
    STRK = '0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d'

    ETH_USDC_ADDRESS = '0xcc2f78f58cd3d242fc7b0661452a7e275f701516459a46b3792616fc270bed'

    WHITELISTED_POOLS = [] # todo
    STABLECOINS = [USDC, USDT, DAI]
    WHITELISTED_TOKENS = [ETH, USDC, DAI, USDT, WBTC, STRK]

ZERO_ADDRESS = '0x0000000000000000000000000000000000000000000000000000000000000000'
ZERO_DECIMAL128 = Decimal128(Decimal(0))
ZERO_DECIMAL = Decimal(0)

DEFAULT_DECIMALS = 18
TIME_INTERVAL = 60  # in seconds
