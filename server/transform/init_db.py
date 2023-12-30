from dataclasses import dataclass
from decimal import Decimal
import os
import sys

from bson import Decimal128
from pymongo import MongoClient


# mainnet contracts
ETH = '0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7'
USDC = '0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8'
DAI = '0x00da114221cb83fa859dbdb4c44beeaa0bb37c7537ad5ae66fe5e0efd20e6eb3'
USDT = '0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8'
WBTC = '0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac'

# testnet contracts
ETH_TESTNET = '0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7'
USDC_TESTNET = '0x5a643907b9a4bc6a55e9069c4fd5fd1f5c79a22470690f75556c4736e34426'
DAI_TESTNET = '0x03e85bfbb8e2a42b7bead9e88e9a1b19dbccf661471061807292120462396ec9'
USDT_TESTNET = '0x06a8f0e0d3bf9a6f049ce3fcbad7e26cbd0ceb3f0bfc6160f4ab7bdd7985b0e3' # not sure
WBTC_TESTNET = '0x012d537dc323c439dc65c976fad242d5610d27cfb5f31689a0a319b8be7f3d56'


@dataclass
class TokenData:
    address: str
    symbol: str
    name: str
    decimals: int = 18

    def to_dict(self) -> dict:
        return {
            'address': self.address,
            'symbol': self.symbol,
            'name': self.name,
            'decimals': self.decimals,
            'totalValueLocked': Decimal128(Decimal(0))
        }


TOKENS_MAPPING = [
    TokenData(ETH, 'ETH', 'Ether').to_dict(),
    TokenData(USDC, 'USDC', 'USD Coin', 6).to_dict(),
    TokenData(DAI, 'DAI', 'Dai Stablecoin').to_dict(),
    TokenData(USDT, 'USDT', 'Tether USD', 6).to_dict(),
    TokenData(WBTC, 'wBTC', 'Wrapped BTC', 8).to_dict(),
]

TOKENS_MAPPING_TESTNET = [
    TokenData(ETH_TESTNET, 'ETH', 'Ether').to_dict(),
    TokenData(USDC_TESTNET, 'USDC', 'USD Coin', 6).to_dict(),
    TokenData(DAI_TESTNET, 'DAI', 'Dai Stablecoin').to_dict(),
    TokenData(USDT_TESTNET, 'USDT', 'Tether USD', 6).to_dict(),
    TokenData(WBTC_TESTNET, 'wBTC', 'Wrapped BTC', 8).to_dict(),
]


def run():
    mongo_url = os.environ.get('MONGO_URL', None)
    if mongo_url is None:
        sys.exit('MONGO_URL not set')
    mongo_database = os.environ.get('MONGO_DB', None)
    if mongo_database is None:
        sys.exit('MONGO_DB not set')
    network = os.environ.get('NETWORK')
    if network not in {'mainnet', 'testnet'}:
        sys.exit("NETWORK shoudl be 'mainnet' or 'testnet'")

    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        collection = db['tokens']
        tokens_mapping = TOKENS_MAPPING if network == 'mainnet' else TOKENS_MAPPING_TESTNET
        
        tokens_to_add = []
        for token in tokens_mapping:
            existing_record = collection.find_one({'address': token['address']})
            if existing_record is None:
                tokens_to_add.append(token)
                print(f'Token will be inserted {token["name"]}')
        if tokens_to_add:
            collection.insert_many(tokens_to_add)
