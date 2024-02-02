import datetime as dt
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database

from server.graphql.resolvers.helpers import (
    BlockFilter, 
    add_block_constraint, 
    convert_timestamp_to_datetime,
    get_liquidity_value
)
from server.const import Collection, ZERO_DECIMAL128
from server.query_utils import filter_by_the_latest_value


@strawberry.type
class PoolCreated:
    id: str

    token0: str
    token1: str
    fee: int
    tickSpacing: int
    poolAddress: str
 
    datetime: dt.datetime
    block: int

    feeGrowthGlobal0X128: Decimal
    feeGrowthGlobal1X128: Decimal
    liquidity: Decimal
    tick: float
    sqrtPriceX96: float

    token0Price: Decimal
    token1Price: Decimal

    totalValueLockedToken0: Decimal
    totalValueLockedToken1: Decimal
    totalValueLockedETH: Decimal
    totalValueLockedUSD: Decimal

    feesUSD: Decimal
    untrackedVolumeUSD: Decimal
    volumeToken0: Decimal
    volumeToken1: Decimal

    txCount: int

    @classmethod
    def from_mongo(cls, data: dict):
        return cls(
            id=data['_id'],
            token0=data['token0'],
            token1=data['token1'],
            fee=data['fee'],
            tickSpacing=data['tickSpacing'],
            poolAddress=data['poolAddress'],
            datetime=convert_timestamp_to_datetime(data['timestamp']),
            block=data['block'],
            feeGrowthGlobal0X128=data['feeGrowthGlobal0X128'].to_decimal(),
            feeGrowthGlobal1X128=data['feeGrowthGlobal1X128'].to_decimal(),
            liquidity=get_liquidity_value(data),
            tick=data['tick'],
            sqrtPriceX96=data['sqrtPriceX96'],
            token0Price=data['token0Price'].to_decimal(),
            token1Price=data['token1Price'].to_decimal(),
            totalValueLockedToken0=data['totalValueLockedToken0'].to_decimal(),
            totalValueLockedToken1=data['totalValueLockedToken1'].to_decimal(),    
            totalValueLockedETH=data['totalValueLockedETH'].to_decimal(),
            totalValueLockedUSD=data['totalValueLockedUSD'].to_decimal(),
            feesUSD=data.get('feesUSD', ZERO_DECIMAL128).to_decimal(),
            untrackedVolumeUSD=data.get('untrackedVolumeUSD', ZERO_DECIMAL128).to_decimal(),
            volumeToken0=data.get('volumeToken0', ZERO_DECIMAL128).to_decimal(),
            volumeToken1=data.get('volumeToken1', ZERO_DECIMAL128).to_decimal(),
            txCount=data.get('txCount', 0),
        )


async def get_pools(info, block: Optional[BlockFilter] = None) -> List[PoolCreated]:
    db: Database = info.context['db']
    query = {}
    filter_by_the_latest_value(query)
    add_block_constraint(query, block)

    cursor = db[Collection.POOLS].find(query)
    return [PoolCreated.from_mongo(d) for d in cursor]
