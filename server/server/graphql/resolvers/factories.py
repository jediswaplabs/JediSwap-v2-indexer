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
    totalVolumeETH: Decimal
    totalVolumeUSD: Decimal
    untrackedVolumeUSD: Decimal
    totalFeesETH: Decimal
    totalFeesUSD: Decimal
    txCount: int

    @classmethod
    def from_mongo(cls, data: dict):
        return cls(
            address=data['address'],    
            totalValueLockedETH=data['totalValueLockedETH'].to_decimal(),
            totalValueLockedUSD=data['totalValueLockedUSD'].to_decimal(),
            totalVolumeETH=data['totalVolumeETH'].to_decimal(),
            totalVolumeUSD=data['totalVolumeUSD'].to_decimal(),
            untrackedVolumeUSD=data['untrackedVolumeUSD'].to_decimal(),
            totalFeesETH=data['totalFeesETH'].to_decimal(),
            totalFeesUSD=data['totalFeesUSD'].to_decimal(),
            txCount=data['txCount'],
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
