import datetime as dt
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import (
    add_order_by_constraint, 
    convert_timestamp_to_datetime, 
    WhereFilterForToken, 
    filter_by_token_address
)
from server.const import Collection


@strawberry.type
class TokenDayData:
    id: str
    tokenAddress: str

    open: Decimal
    close: Decimal
    high: Decimal
    low: Decimal

    feesUSD: Decimal
    priceUSD: Decimal

    dayId: int
    datetime: dt.datetime

    totalValueLocked: Decimal
    totalValueLockedUSD: Decimal
    untrackedVolumeUSD: Decimal
    volume: Decimal
    volumeUSD: Decimal

    @classmethod
    def from_mongo(cls, data):
        return cls(
            id=data['_id'],
            tokenAddress=data['tokenAddress'],
            open=data['open'].to_decimal(),
            close=data['close'].to_decimal(),
            high=data['high'].to_decimal(),
            low=data['low'].to_decimal(),
            feesUSD=data['feesUSD'].to_decimal(),
            priceUSD=data['priceUSD'].to_decimal(),
            dayId=data['dayId'],
            datetime=convert_timestamp_to_datetime(data['date']),
            totalValueLocked=data['totalValueLocked'].to_decimal(),
            totalValueLockedUSD=data['totalValueLockedUSD'].to_decimal(),
            untrackedVolumeUSD=data['untrackedVolumeUSD'].to_decimal(),
            volume=data['volume'].to_decimal(),
            volumeUSD=data['volumeUSD'].to_decimal(),
        )


async def get_tokens_day_data(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForToken] = None
) -> List[TokenDayData]:
    db: Database = info.context['db']
    query = {}

    if where is not None:
        filter_by_token_address(where, query)

    cursor = db[Collection.TOKENS_DAY_DATA].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [TokenDayData.from_mongo(d) for d in cursor]