from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database

from server.graphql.resolvers.helpers import BlockFilter, add_block_constraint
from server.const import Collection, ZERO_DECIMAL128
from server.query_utils import filter_by_the_latest_value
from server.utils import convert_bigint_field


@strawberry.type
class PoolCreated:
    id: str

    fee: int
    tickSpacing: int
    poolAddress: str
    token0: str
    token1: str
    liquidity: Decimal
    tick: Optional[Decimal] or None
    sqrtPriceX96: Optional[Decimal] or None
    timestamp: str
    block: int

    totalValueLockedToken0: Decimal
    totalValueLockedToken1: Decimal
    totalValueLockedETH: Decimal
    totalValueLockedUSD: Decimal

    @classmethod
    def from_mongo(cls, data: dict):
        tick = data.get('tick')
        if tick is not None:
            tick = Decimal(convert_bigint_field(tick))
            
        return cls(
            id=data['_id'],
            fee=data['fee'],
            tickSpacing=data['tickSpacing'],
            poolAddress=data['poolAddress'],
            token0=data['token0'],
            token1=data['token1'],
            liquidity=data.get('liquidity', ZERO_DECIMAL128).to_decimal(),
            sqrtPriceX96=data.get('sqrtPriceX96'),
            tick=tick,
            timestamp=data['timestamp'],
            block=data['block'],
            totalValueLockedToken0=data.get('totalValueLockedToken0', ZERO_DECIMAL128).to_decimal(),
            totalValueLockedToken1=data.get('totalValueLockedToken1', ZERO_DECIMAL128).to_decimal(),    
            totalValueLockedETH=data.get('totalValueLockedETH', ZERO_DECIMAL128).to_decimal(),
            totalValueLockedUSD=data.get('totalValueLockedUSD', ZERO_DECIMAL128).to_decimal(),
        )


async def get_pools(info, block: Optional[BlockFilter] = None) -> List[PoolCreated]:
    db: Database = info.context['db']
    query = {}
    filter_by_the_latest_value(query)
    add_block_constraint(query, block)

    cursor = db[Collection.POOLS].find(query)
    return [PoolCreated.from_mongo(d) for d in cursor]
