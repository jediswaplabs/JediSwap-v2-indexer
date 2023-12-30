from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database

from server.resolvers.helpers import BlockFilter, add_block_constraint
from const import Collection
from utils import filter_by_the_latest_value, convert_bigint_field


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

    @classmethod
    def from_mongo(cls, data: dict):
        tick = data.get('tick')
        if tick:
            tick = Decimal(convert_bigint_field(tick))
        sqrtPriceX96 = data.get('sqrtPriceX96')
        if sqrtPriceX96:
            sqrtPriceX96 = Decimal(sqrtPriceX96)
            
        return cls(
            id=data['_id'],
            fee=data['fee'],
            tickSpacing=data['tickSpacing'],
            poolAddress=data['poolAddress'],
            token0=data['token0'],
            token1=data['token1'],
            liquidity=Decimal(data.get('liquidity', 0)),
            sqrtPriceX96=sqrtPriceX96,
            tick=tick,
            timestamp=data['timestamp'],
            block=data['block'],
        )


async def get_pools(info, block: Optional[BlockFilter] = None) -> List[PoolCreated]:
    db: Database = info.context['db']
    query = {}
    filter_by_the_latest_value(query)
    add_block_constraint(query, block)

    cursor = db[Collection.POOLS].find(query)
    return [PoolCreated.from_mongo(d) for d in cursor]
