import datetime as dt
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import (
    add_order_by_constraint, 
)
from server.const import Collection


@strawberry.type
class FactoryDayData:
    id: str

    dayId: int
    datetime: dt.datetime

    feesUSD: Decimal
    totalValueLockedUSD: Decimal
    volumeETH: Decimal
    volumeUSD: Decimal

    txCount: int

    @classmethod
    def from_mongo(cls, data):
        return cls(
            id=data['_id'],
            dayId=data['dayId'],
            datetime=data['date'],
            feesUSD=data['feesUSD'].to_decimal(),
            totalValueLockedUSD=data['totalValueLockedUSD'].to_decimal(),
            volumeETH=data['volumeETH'].to_decimal(),
            volumeUSD=data['volumeUSD'].to_decimal(),
            txCount=data.get('txCount', 0),
        )


async def get_factories_day_data(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc') -> List[FactoryDayData]:
    db: Database = info.context['db']
    query = {}

    cursor = db[Collection.FACTORIES_DAY_DATA].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [FactoryDayData.from_mongo(d) for d in cursor]
