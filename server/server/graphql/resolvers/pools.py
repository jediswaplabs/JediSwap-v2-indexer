import datetime as dt
from decimal import Decimal
from typing import List, Optional

import strawberry
from strawberry.types import Info
from pymongo.database import Database

from server.graphql.resolvers.helpers import (
    BlockFilter,
    WhereFilterForPool,
    add_block_constraint,
    add_order_by_constraint, 
    convert_timestamp_to_datetime,
    filter_pools
)
from server.const import Collection, ZERO_DECIMAL128
from server.graphql.resolvers.tokens import Token
from server.query_utils import filter_by_the_latest_value


@strawberry.type
class Pool:
    poolAddress: str

    fee: int
    tickSpacing: int
 
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
    volumeUSD: Decimal
    volumeToken0: Decimal
    volumeToken1: Decimal

    txCount: int

    token0Address: strawberry.Private[str]
    token1Address: strawberry.Private[str]

    @strawberry.field
    def token0(self, info: Info) -> Token:
        return info.context["token_loader"].load(self.token0Address)

    @strawberry.field
    def token1(self, info: Info) -> Token:
        return info.context["token_loader"].load(self.token1Address)

    @classmethod
    def from_mongo(cls, data: dict):
        return cls(
            poolAddress=data['poolAddress'],
            fee=data['fee'],
            tickSpacing=data['tickSpacing'],
            datetime=convert_timestamp_to_datetime(data['timestamp']),
            block=data['block'],
            feeGrowthGlobal0X128=Decimal(int(data.get('feeGrowthGlobal0X128', '0x0'), 16)),
            feeGrowthGlobal1X128=Decimal(int(data.get('feeGrowthGlobal1X128', '0x0'), 16)),
            liquidity=data.get('liquidity', ZERO_DECIMAL128).to_decimal(),
            tick=data.get('tick', 0),
            sqrtPriceX96=data.get('sqrtPriceX96', 0),
            token0Price=data.get('token0Price', ZERO_DECIMAL128).to_decimal(),
            token1Price=data.get('token1Price', ZERO_DECIMAL128).to_decimal(),
            totalValueLockedToken0=data.get('totalValueLockedToken0', ZERO_DECIMAL128).to_decimal(),
            totalValueLockedToken1=data.get('totalValueLockedToken1', ZERO_DECIMAL128).to_decimal(),    
            totalValueLockedETH=data.get('totalValueLockedETH', ZERO_DECIMAL128).to_decimal(),
            totalValueLockedUSD=data.get('totalValueLockedUSD', ZERO_DECIMAL128).to_decimal(),
            feesUSD=data.get('feesUSD', ZERO_DECIMAL128).to_decimal(),
            untrackedVolumeUSD=data.get('untrackedVolumeUSD', ZERO_DECIMAL128).to_decimal(),
            volumeUSD=data.get('volumeUSD', ZERO_DECIMAL128).to_decimal(),
            volumeToken0=data.get('volumeToken0', ZERO_DECIMAL128).to_decimal(),
            volumeToken1=data.get('volumeToken1', ZERO_DECIMAL128).to_decimal(),
            txCount=data.get('txCount', 0),
            token0Address = data['token0'],
            token1Address = data['token1'],
        )


async def get_pools(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = "asc", block: Optional[BlockFilter] = None, where: Optional[WhereFilterForPool] = None
) -> List[Pool]:
    db: Database = info.context['db']
    query = {}
    filter_by_the_latest_value(query)
    await add_block_constraint(query, block)

    if where is not None:
        await filter_pools(where, query)

    cursor = db[Collection.POOLS].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)
    return [Pool.from_mongo(d) for d in cursor]

async def get_pool(db: Database, id: str) -> Pool:
    query = {'poolAddress': id}
    pool = db['pools'].find_one(query)
    return Pool.from_mongo(pool)