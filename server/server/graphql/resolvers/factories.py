from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database

from server.graphql.resolvers.helpers import BlockFilter, add_block_constraint
from server.const import Collection, ZERO_DECIMAL128


@strawberry.type
class Factory:
    address: str

    totalValueLockedETH: Decimal
    totalValueLockedUSD: Decimal

    @classmethod
    def from_mongo(cls, data: dict):
        return cls(
            address=data['address'],    
            totalValueLockedETH=data.get('totalValueLockedETH', ZERO_DECIMAL128).to_decimal(),
            totalValueLockedUSD=data.get('totalValueLockedUSD', ZERO_DECIMAL128).to_decimal(),
        )


@strawberry.input
class FactoryFilter:
    address: Optional[str] = None


async def get_factories(info, block: Optional[BlockFilter] = None, where: Optional[FactoryFilter] = None) -> List[Factory]:
    db: Database = info.context['db']
    query = {}

    if where is not None:
        if where.address is not None:
            query['address'] = where.address

    add_block_constraint(query, block)

    cursor = db[Collection.FACTORIES].find(query)
    return [Factory.from_mongo(d) for d in cursor]
