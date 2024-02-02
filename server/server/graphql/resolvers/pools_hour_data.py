import datetime as dt
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import (
    add_order_by_constraint, 
    convert_timestamp_to_datetime, 
    WhereFilterForPool, 
    filter_by_pool_address,
    get_liquidity_value
)
from server.const import Collection


@strawberry.type
class PoolHourData:
    id: str
    poolAddress: str

    open: Decimal
    close: Decimal
    high: Decimal
    low: Decimal

    feesUSD: Decimal
    liquidity: Decimal
    feeGrowthGlobal0X128: Decimal
    feeGrowthGlobal1X128: Decimal
    sqrtPriceX96: float
    tick: int

    hourId: int
    datetime: dt.datetime

    token0Price: Decimal
    token1Price: Decimal

    totalValueLockedUSD: Decimal
    volumeToken0: Decimal
    volumeToken1: Decimal
    volumeUSD: Decimal

    txCount: int

    @classmethod
    def from_mongo(cls, data):
        return cls(
            id=data['_id'],
            poolAddress=data['poolAddress'],
            open=data['open'].to_decimal(),
            close=data['close'].to_decimal(),
            high=data['high'].to_decimal(),
            low=data['low'].to_decimal(),
            feesUSD=data['feesUSD'].to_decimal(),
            liquidity=get_liquidity_value(data),
            feeGrowthGlobal0X128=data['feeGrowthGlobal0X128'].to_decimal(),
            feeGrowthGlobal1X128=data['feeGrowthGlobal1X128'].to_decimal(),
            sqrtPriceX96=data['sqrtPriceX96'],
            tick=data['tick'],
            hourId=data['hourId'],
            datetime=convert_timestamp_to_datetime(data['periodStartUnix']),
            token0Price=data['token0Price'].to_decimal(),
            token1Price=data['token1Price'].to_decimal(),
            totalValueLockedUSD=data['totalValueLockedUSD'].to_decimal(),
            volumeToken0=data['volumeToken0'].to_decimal(),
            volumeToken1=data['volumeToken1'].to_decimal(),
            volumeUSD=data['volumeUSD'].to_decimal(),
            txCount=data.get('txCount', 0),
        )


async def get_pools_hour_data(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForPool] = None
) -> List[PoolHourData]:
    db: Database = info.context['db']
    query = {}

    if where is not None:
        filter_by_pool_address(where, query)

    cursor = db[Collection.POOLS_HOUR_DATA].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [PoolHourData.from_mongo(d) for d in cursor]
