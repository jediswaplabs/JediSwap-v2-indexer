from dataclasses import dataclass

from bson import Decimal128
from pymongo import MongoClient, UpdateOne
from pymongo.database import Database

from server.const import Collection, FACTORY_ADDRESS, ETH, USDC, DAI, USDT, WBTC, ZERO_DECIMAL128
from server.pricing import EthPrice, sqrt_price_x96_to_token_prices, find_eth_per_token
from server.query_utils import get_tokens_from_pool


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
        pools_collection = db[Collection.POOLS]

        # add tokens
        tokens_to_add = []
        for token in TOKENS_MAPPING:
            existing_record = tokens_collection.find_one({'address': token['address']})
            if existing_record is None:
                tokens_to_add.append(token)
                print(f'Token will be inserted {token["name"]}')
        if tokens_to_add:
            tokens_collection.insert_many(tokens_to_add)

        # add factory
        existing_factory_record = factory_collection.find_one({'address': FACTORY_ADDRESS})
        if existing_factory_record is None:
            factory_collection.insert_one(FACTORY_RECORD)
            print('Factory record inserted')

        # add tokens price for pools
        EthPrice.set(rpc_url)
        pools_update_operations = []
        tokens_update_operations = []
        for pool in pools_collection.find({
            "$and": [
                {'sqrtPriceX96': {"$exists": True}}, 
                {'sqrtPriceX96': {"$ne": None}},
                ]}):
            token0, token1 = get_tokens_from_pool(db, pool)
            price0, price1 = sqrt_price_x96_to_token_prices(pool['sqrtPriceX96'], token0['decimals'], token1['decimals'])
            pools_update_operations = []
            pools_update_operations.append(
                UpdateOne({"_id": pool['_id']}, {
                    "$set": {
                        "price0": Decimal128(price0),
                        "price1": Decimal128(price1),
            }}))

            # update tokens price
            token0_derivedETH = find_eth_per_token(db, token0['address'])
            token1_derivedETH = find_eth_per_token(db, token1['address'])
            tokens_update_operations.extend([
                UpdateOne({"_id": token0['_id']}, {"$set": {"derivedETH": Decimal128(token0_derivedETH)}}),
                UpdateOne({"_id": token1['_id']}, {"$set": {"derivedETH": Decimal128(token1_derivedETH)}}),
            ])
        if pools_update_operations:
            pools_collection.bulk_write(pools_update_operations)
        if tokens_update_operations:
            tokens_collection.bulk_write(tokens_update_operations)
