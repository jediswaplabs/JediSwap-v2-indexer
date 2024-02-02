from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import (
    add_order_by_constraint, 
    WhereFilterForToken, 
    filter_by_token_address,
    filter_by_last_seven_days,
)
from server.const import Collection


@strawberry.type
class TokenSevenDaysData:
    tokenAddress: str

    feesUSD: Decimal

    totalValueLocked: Decimal
    totalValueLockedUSD: Decimal
    untrackedVolumeUSD: Decimal
    volume: Decimal
    volumeUSD: Decimal

    @classmethod
    def from_mongo(cls, data):
        return cls(
            tokenAddress=data['tokenAddress'],
            feesUSD=data['feesUSD'],
            totalValueLocked=data['totalValueLocked'],
            totalValueLockedUSD=data['totalValueLockedUSD'],
            untrackedVolumeUSD=data['untrackedVolumeUSD'],
            volume=data['volume'],
            volumeUSD=data['volumeUSD'],
        )


async def get_tokens_seven_days_data(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForToken] = None
) -> List[TokenSevenDaysData]:
    db: Database = info.context['db']
    query = {}

    if where is not None:
        filter_by_token_address(where, query)

    filter_by_last_seven_days(query)

    cursor = db[Collection.TOKENS_DAY_DATA].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    data = {}
    for db_record in cursor:
        token_address = db_record['tokenAddress']
        if token_address in data:
            data[token_address]['feesUSD'] += db_record['feesUSD'].to_decimal()
            data[token_address]['totalValueLocked'] +=  db_record['totalValueLocked'].to_decimal()
            data[token_address]['totalValueLockedUSD'] += db_record['totalValueLockedUSD'].to_decimal()
            data[token_address]['untrackedVolumeUSD'] += db_record['untrackedVolumeUSD'].to_decimal()
            data[token_address]['volume'] += db_record['volume'].to_decimal()
            data[token_address]['volumeUSD'] += db_record['volumeUSD'].to_decimal()
        else:
            data[token_address] = {}
            data[token_address]['tokenAddress'] = token_address
            data[token_address]['feesUSD'] = db_record['feesUSD'].to_decimal()
            data[token_address]['totalValueLocked'] =  db_record['totalValueLocked'].to_decimal()
            data[token_address]['totalValueLockedUSD'] = db_record['totalValueLockedUSD'].to_decimal()
            data[token_address]['untrackedVolumeUSD'] = db_record['untrackedVolumeUSD'].to_decimal()
            data[token_address]['volume'] = db_record['volume'].to_decimal()
            data[token_address]['volumeUSD'] = db_record['volumeUSD'].to_decimal()

    return [TokenSevenDaysData.from_mongo(values) for _, values in data.items()]
