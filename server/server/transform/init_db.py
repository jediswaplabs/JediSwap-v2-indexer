from dataclasses import dataclass

from pymongo import MongoClient
from pymongo.database import Database

from server.const import Collection, FACTORY_ADDRESS, ETH, USDC, DAI, USDT, WBTC, ZERO_DECIMAL128


@dataclass
class TokenData:
    address: str
    symbol: str
    name: str
    decimals: int = 18

    def to_dict(self) -> dict:
        return {
            'tokenAddress': self.address,
            'symbol': self.symbol,
            'name': self.name,
            'decimals': self.decimals,
            'derivedETH': ZERO_DECIMAL128,
            'totalValueLocked': ZERO_DECIMAL128,
            'totalValueLockedUSD': ZERO_DECIMAL128,
        }


TOKENS_MAPPING = [
    TokenData(ETH, 'ETH', 'Ether').to_dict(),
    TokenData(USDC, 'USDC', 'USD Coin', 6).to_dict(),
    TokenData(DAI, 'DAI', 'Dai Stablecoin').to_dict(),
    TokenData(USDT, 'USDT', 'Tether USD', 6).to_dict(),
    TokenData(WBTC, 'wBTC', 'Wrapped BTC', 8).to_dict(),
]


FACTORY_RECORD = {
    'address': FACTORY_ADDRESS,
    'txCount': 0,
    'totalValueLockedETH': ZERO_DECIMAL128,
    'totalValueLockedUSD': ZERO_DECIMAL128,
    'totalVolumeETH': ZERO_DECIMAL128,
    'totalVolumeUSD': ZERO_DECIMAL128,
    'untrackedVolumeUSD': ZERO_DECIMAL128,
    'totalFeesETH': ZERO_DECIMAL128,
    'totalFeesUSD': ZERO_DECIMAL128,
}


def run(mongo_url: str, mongo_database: Database, rpc_url: str):
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]
        tokens_collection = db[Collection.TOKENS]
        factory_collection = db[Collection.FACTORIES]

        # add tokens
        tokens_to_add = []
        for token in TOKENS_MAPPING:
            existing_record = tokens_collection.find_one({'tokenAddress': token['tokenAddress']})
            if existing_record is None:
                tokens_to_add.append(token)
                print(f'Token will be inserted {token["name"]}')
        if tokens_to_add:
            tokens_collection.insert_many(tokens_to_add)
