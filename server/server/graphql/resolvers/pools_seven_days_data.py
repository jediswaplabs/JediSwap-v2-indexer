from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import (
    add_order_by_constraint, 
    WhereFilterForPool, 
    filter_by_pool_address,
    filter_by_last_seven_days,
    get_liquidity_value
)
from server.const import Collection


@strawberry.type
class PoolSevenDaysData:
    poolAddress: str

    feesUSD: Decimal
    liquidity: Decimal

    totalValueLockedUSD: Decimal
    volumeToken0: Decimal
    volumeToken1: Decimal
    volumeUSD: Decimal

    @classmethod
    def from_mongo(cls, data):
        return cls(
            poolAddress=data['_id'],
            feesUSD=data['feesUSD'].to_decimal(),
            liquidity=get_liquidity_value(data),
            totalValueLockedUSD=data['totalValueLockedUSD'].to_decimal(),
            volumeToken0=data['volumeToken0'].to_decimal(),
            volumeToken1=data['volumeToken1'].to_decimal(),
            volumeUSD=data['volumeUSD'].to_decimal(),
        )


async def get_pools_seven_days_data(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForPool] = None
) -> List[PoolSevenDaysData]:
    db: Database = info.context['db']
    match_query = {}

    if where is not None:
        filter_by_pool_address(where, match_query)

    filter_by_last_seven_days(match_query)

    pipeline = [
        {
            "$match": match_query
        },
        {
            "$group": {
                "_id": "$poolAddress",
                "feesUSD": {"$sum": "$feesUSD"},
                "liquidity": {"$sum": "$liquidity"},
                "totalValueLockedUSD": {"$sum": "$totalValueLockedUSD"},
                "volumeToken0": {"$sum": "$volumeToken0"},
                "volumeToken1": {"$sum": "$volumeToken1"},
                "volumeUSD": {"$sum": "$volumeUSD"},
            }
        }
    ]

    cursor = db[Collection.POOLS_HOUR_DATA ].aggregate(pipeline)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)
    return [PoolSevenDaysData.from_mongo(d) for d in cursor]
