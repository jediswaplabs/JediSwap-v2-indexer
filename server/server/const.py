from decimal import Decimal
import os

from bson import Decimal128


class Collection:
    POOLS = 'pools'
    POOLS_DATA = 'pools_data'
    POSITIONS = 'positions'
    POSITION_FEES = 'position_fees'
    TOKENS = 'tokens'
    FACTORIES = 'factories'


# mainnet contracts
# should start with 0x0...
if os.environ.get('NETWORK') == 'mainnet':
    FACTORY_ADDRESS = '' # todo
    NFT_ROUTER = '' # todo
    
    ETH = '0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7'
    USDC = '0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8'
    DAI = '0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3'
    USDT = '0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8'
    WBTC = '0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac'

    ETH_USDC_ADDRESS = ''   # todo
    WHITELISTED_POOLS = []  # todo
else:
    # testnet contracts
    FACTORY_ADDRESS = '0x06262409329bff003489ccac5d548bb75d33c896e29ceb6a586084a266e094ff'
    NFT_ROUTER = '0x067c1ae6f84275a929accf49122c86531259ffb01d3e2a6bf72729ca05566547'

    ETH = '0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7'
    USDC = '0x05a643907b9a4bc6a55e9069c4fd5fd1f5c79a22470690f75556c4736e34426'
    DAI = '0x03e85bfbb8e2a42b7bead9e88e9a1b19dbccf661471061807292120462396ec9'
    USDT = '0x06a8f0e0d3bf9a6f049ce3fcbad7e26cbd0ceb3f0bfc6160f4ab7bdd7985b0e3' # not sure
    WBTC = '0x012d537dc323c439dc65c976fad242d5610d27cfb5f31689a0a319b8be7f3d56'

    ETH_USDC_ADDRESS = '0x0687959c1ab64e1d3df1825dfec5a650f18af44060b29e6a50643c770b15545c'

    WHITELISTED_POOLS = [
        '0x015b5061cab98c4b3a7d26cba1ae33636337c5bc8e17ba4cd9dcc5c9bab2c615',
    ]


STABLECOINS = [USDC, USDT, DAI]

ZERO_ADDRESS = '0x0000000000000000000000000000000000000000000000000000000000000000'
ZERO_DECIMAL128 = Decimal128(Decimal(0))

DEFAULT_DECIMALS = 18
