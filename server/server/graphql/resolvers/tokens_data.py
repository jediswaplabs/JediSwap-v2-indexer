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
from server.graphql.resolvers.tokens import Token


async def add_empty_token_data(tokens: dict, token_address: str, periods: list):
    tokens[token_address] = {}
    tokens[token_address]['tokenAddress'] = token_address
    tokens[token_address]['period'] = {}
    for period in periods:
        tokens[token_address]['period'][period] = {}


@strawberry.type
class TokenData:
    period: strawberry.scalars.JSON

    tokenAddress: strawberry.Private[str]
    @strawberry.field
    def token(self, info: Info) -> Token:
        return info.context["token_loader"].load(self.tokenAddress)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            tokenAddress=data['tokenAddress'],
            period=data['period'],
        )


@strawberry.input
class WhereFilterForTokenAndPeriod:
    token_address: Optional[str] = None
    token_address_in: Optional[List[str]] = field(default_factory=list)
    period_in: Optional[List[str]] = field(default_factory=list)


async def get_tokens_data(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForTokenAndPeriod] = None
) -> List[TokenData]:
    db: Database = info.context['db']

    periods = await validate_period_input(where)

    query = {}
    if where is not None:
        await filter_by_token_address(where, query)
        
    cursor = db[Collection.TOKENS].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)

    tokens = {}
    tokens_addresses = []
    for record in cursor:
        token_address = record['tokenAddress']
        await add_empty_token_data(tokens, token_address, periods)
        tokens_addresses.append(token_address)

    for period_name in periods:
        period_query = {
            'tokenAddress': {'$in': tokens_addresses}
        }
        await filter_by_days_data(period_query, period_name)
        pipeline = [
            {
                "$match": period_query
            },
            {
                "$group": {
                    "_id": "$tokenAddress",
                    "feesUSD": {"$sum": "$feesUSD"},
                    "totalValueLocked": {"$last": "$totalValueLocked"},
                    "totalValueLockedUSD": {"$last": "$totalValueLockedUSD"},
                    "totalValueLockedUSDFirst": {"$first": "$totalValueLockedUSD"},
                    "derivedETH": {"$last": "$derivedETH"},
                    "untrackedVolumeUSD": {"$sum": "$untrackedVolumeUSD"},
                    "volume": {"$sum": "$volume"},
                    "volumeUSD": {"$sum": "$volumeUSD"},
                    "txCount": {"$sum": "$txCount"},
                    "open": {"$first": "$open"},
                    "high": {"$max": "$high"},
                    "low": {"$min": "$low"},
                    "close": {"$last": "$close"},
                }
            }
        ]

        cursor = db[Collection.TOKENS_HOUR_DATA ].aggregate(pipeline)
        for record in cursor:
            token_address = record['_id']
            tokens[token_address]['period'][period_name] = {
                'feesUSD': str(record['feesUSD'].to_decimal()),
                'totalValueLocked': str(record['totalValueLocked'].to_decimal()),
                'totalValueLockedUSD': str(record['totalValueLockedUSD'].to_decimal()),
                'totalValueLockedUSDFirst': str(record['totalValueLockedUSDFirst'].to_decimal()),
                'derivedETH': str(record['derivedETH'].to_decimal()),
                'untrackedVolumeUSD': str(record['untrackedVolumeUSD'].to_decimal()),
                'volume': str(record['volume'].to_decimal()),
                'volumeUSD': str(record['volumeUSD'].to_decimal()),
                'txCount': str(record['txCount']),
                'open': str(record['open'].to_decimal()),
                'high': str(record['high'].to_decimal()),
                'low': str(record['low'].to_decimal()),
                'close': str(record['close'].to_decimal()),
            }

    return [TokenData.from_mongo(values) for _, values in tokens.items()]
