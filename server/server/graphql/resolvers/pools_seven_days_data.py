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
            poolAddress=data['poolAddress'],
            feesUSD=data['feesUSD'],
            liquidity=data['liquidity'],
            totalValueLockedUSD=data['totalValueLockedUSD'],
            volumeToken0=data['volumeToken0'],
            volumeToken1=data['volumeToken1'],
            volumeUSD=data['volumeUSD'],
        )


async def get_pools_seven_days_data(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForPool] = None
) -> List[PoolSevenDaysData]:
    db: Database = info.context['db']
    query = {}

    if where is not None:
        filter_by_pool_address(where, query)

    filter_by_last_seven_days(query)

    cursor = db[Collection.POOLS_DAY_DATA].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    data = {}
    for db_record in cursor:
        pool_address = db_record['poolAddress']
        if pool_address in data:
            data[pool_address]['feesUSD'] += db_record['feesUSD'].to_decimal()
            data[pool_address]['liquidity'] += get_liquidity_value(db_record)
            data[pool_address]['totalValueLockedUSD'] += db_record['totalValueLockedUSD'].to_decimal()
            data[pool_address]['volumeToken0'] += db_record['volumeToken0'].to_decimal()
            data[pool_address]['volumeToken1'] += db_record['volumeToken1'].to_decimal()
            data[pool_address]['volumeUSD'] += db_record['volumeUSD'].to_decimal()
        else:
            data[pool_address] = {}
            data[pool_address]['poolAddress'] = pool_address
            data[pool_address]['feesUSD'] = db_record['feesUSD'].to_decimal()
            data[pool_address]['liquidity'] = get_liquidity_value(db_record)
            data[pool_address]['totalValueLockedUSD'] = db_record['totalValueLockedUSD'].to_decimal()
            data[pool_address]['volumeToken0'] = db_record['volumeToken0'].to_decimal()
            data[pool_address]['volumeToken1'] = db_record['volumeToken1'].to_decimal()
            data[pool_address]['volumeUSD'] = db_record['volumeUSD'].to_decimal()

    return [PoolSevenDaysData.from_mongo(values) for _, values in data.items()]
