from dataclasses import field
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import (
    add_order_by_constraint, 
    filter_by_token_address,
    filter_by_days_data,
    validate_period_input,
)
from server.const import Collection


def add_empty_token_data(tokens: dict, token_address: str, periods: list):
    tokens[token_address] = {}
    tokens[token_address]['tokenAddress'] = token_address
    tokens[token_address]['period'] = {}
    for period in periods:
        tokens[token_address]['period'][period] = {}


@strawberry.type
class TokenData:
    tokenAddress: str
    period: strawberry.scalars.JSON

    @classmethod
    def from_mongo(cls, data):
        return cls(
            tokenAddress=data['tokenAddress'],
            period=data['period'],
        )


@strawberry.input
class WhereFilterForTokenData:
    token_address: Optional[str] = None
    token_address_in: Optional[List[str]] = field(default_factory=list)
    period_in: Optional[List[str]] = field(default_factory=list)


async def get_tokens_data(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForTokenData] = None
) -> List[TokenData]:
    db: Database = info.context['db']

    periods = validate_period_input(where)

    query = {}
    if where is not None:
        filter_by_token_address(where, query)
        
    tokens = {}
    cursor = db[Collection.TOKENS].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)
    tokens_addresses = [record['tokenAddress'] for record in cursor]

    for period_name in periods:
        period_query = {
            'tokenAddress': {'$in': tokens_addresses}
        }
        filter_by_days_data(period_query, period_name)
        pipeline = [
            {
                "$match": period_query
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
        for record in cursor:
            token_address = record['_id']
            if token_address not in tokens:
                add_empty_token_data(tokens, token_address, periods)

            tokens[token_address]['period'][period_name] = {
                'feesUSD': str(record['feesUSD'].to_decimal()),
                'totalValueLocked': str(record['totalValueLocked'].to_decimal()),
                'totalValueLockedUSD': str(record['totalValueLockedUSD'].to_decimal()),
                'untrackedVolumeUSD': str(record['untrackedVolumeUSD'].to_decimal()),
                'volume': str(record['volume'].to_decimal()),
                'volumeUSD': str(record['volumeUSD'].to_decimal()),
            }

    if not tokens:
        for token_address in tokens_addresses:
            add_empty_token_data(tokens, token_address, periods)

    return [TokenData.from_mongo(values) for _, values in tokens.items()]
