from dataclasses import field
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import (
    add_order_by_constraint, 
    filter_by_pool_address,
    filter_by_days_data,
    validate_period_input,
    filter_pools_by_token_addresses
)
from server.const import Collection
from server.utils import format_address
from server.graphql.resolvers.pools import Pool


def add_empty_pool_data(pools: dict, pool_address: str, periods: list):
    pools[pool_address] = {}
    pools[pool_address]['poolAddress'] = pool_address
    pools[pool_address]['period'] = {}
    for period in periods:
        pools[pool_address]['period'][period] = {}

@strawberry.type
class PoolData:
    period: strawberry.scalars.JSON

    poolAddress: strawberry.Private[str]
    @strawberry.field
    def pool(self, info: Info) -> Pool:
        return info.context["pool_loader"].load(self.poolAddress)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            poolAddress=data['poolAddress'],
            period=data['period'],
        )


@strawberry.input
class WhereFilterForPoolAndPeriodAndTokens:
    pool_address: Optional[str] = None
    pool_address_in: Optional[List[str]] = field(default_factory=list)
    period_in: Optional[List[str]] = field(default_factory=list)
    both_token_address_in: Optional[List[str]] = field(default_factory=list)


async def get_pools_data(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForPoolAndPeriodAndTokens] = None
) -> List[PoolData]:
    db: Database = info.context['db']

    periods = await validate_period_input(where)

    query = {}
    if where is not None:
        await filter_pools_by_token_addresses(where, query, db)
        await filter_by_pool_address(where, query)
        
    cursor = db[Collection.POOLS].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)

    pools = {}
    pools_addresses = []
    for record in cursor:
        pool_address = record['poolAddress']
        add_empty_pool_data(pools, pool_address, periods)
        pools_addresses.append(pool_address)
    
    for period_name in periods:
        period_query = {
            'poolAddress': {'$in': pools_addresses}
        }
        await filter_by_days_data(period_query, period_name)
        pipeline = [
            {
                "$match": period_query
            },
            {
                "$group": {
                    "_id": "$poolAddress",
                    "feesUSD": {"$sum": "$feesUSD"},
                    "liquidity": {"$last": "$liquidity"},
                    "totalValueLockedUSD": {"$last": "$totalValueLockedUSD"},
                    "volumeToken0": {"$sum": "$volumeToken0"},
                    "volumeToken1": {"$sum": "$volumeToken1"},
                    "volumeUSD": {"$sum": "$volumeUSD"},
                    }
            }
        ]

        cursor = db[Collection.POOLS_HOUR_DATA ].aggregate(pipeline)
        for record in cursor:
            pool_address = record['_id']
            pools[pool_address]['period'][period_name] = {
                'feesUSD': str(record['feesUSD'].to_decimal()),
                'liquidity': str(record['liquidity'].to_decimal()),
                'totalValueLockedUSD': str(record['totalValueLockedUSD'].to_decimal()),
                'volumeToken0': str(record['volumeToken0'].to_decimal()),
                'volumeToken1': str(record['volumeToken1'].to_decimal()),
                'volumeUSD': str(record['volumeUSD'].to_decimal()),
            }

    return [PoolData.from_mongo(values) for _, values in pools.items()]
