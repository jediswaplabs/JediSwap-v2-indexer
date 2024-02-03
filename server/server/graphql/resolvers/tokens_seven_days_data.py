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
            tokenAddress=data['_id'],
            feesUSD=data['feesUSD'].to_decimal(),
            totalValueLocked=data['totalValueLocked'].to_decimal(),
            totalValueLockedUSD=data['totalValueLockedUSD'].to_decimal(),
            untrackedVolumeUSD=data['untrackedVolumeUSD'].to_decimal(),
            volume=data['volume'].to_decimal(),
            volumeUSD=data['volumeUSD'].to_decimal(),
        )


async def get_tokens_seven_days_data(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForToken] = None
) -> List[TokenSevenDaysData]:
    db: Database = info.context['db']

    match_query = {}
    if where is not None:
        filter_by_token_address(where, match_query)

    filter_by_last_seven_days(match_query)

    pipeline = [
        {
            "$match": match_query
        },
        {
            "$group": {
                "_id": "$tokenAddress",
                "feesUSD": {"$sum": "$feesUSD"},
                "totalValueLocked": {"$sum": "$totalValueLocked"},
                "totalValueLockedUSD": {"$sum": "$totalValueLockedUSD"},
                "untrackedVolumeUSD": {"$sum": "$untrackedVolumeUSD"},
                "volume": {"$sum": "$volume"},
                "volumeUSD": {"$sum": "$volumeUSD"},
            }
        }
    ]

    cursor = db[Collection.TOKENS_HOUR_DATA ].aggregate(pipeline)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)
    return [TokenSevenDaysData.from_mongo(d) for d in cursor]