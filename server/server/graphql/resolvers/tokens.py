from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import add_order_by_constraint, WhereFilterForToken
from server.const import Collection, ZERO_DECIMAL128


@strawberry.type
class Token:
    tokenAddress: str

    name: str
    symbol: str
    decimals: int

    totalValueLocked: Decimal
    totalValueLockedUSD: Decimal
    derivedETH: Decimal
 
    feesUSD: Decimal
    untrackedVolumeUSD: Decimal
    volume: Decimal
    volumeUSD: Decimal

    txCount: int

    @classmethod
    def from_mongo(cls, data):
        return cls(
            tokenAddress=data['tokenAddress'],
            name=data['name'],
            symbol=data['symbol'],
            decimals=data['decimals'],
            totalValueLocked=data['totalValueLocked'].to_decimal(),
            totalValueLockedUSD=data['totalValueLockedUSD'].to_decimal(),
            derivedETH=data['derivedETH'].to_decimal(),
            feesUSD=data.get('feesUSD', ZERO_DECIMAL128).to_decimal(),
            untrackedVolumeUSD=data.get('untrackedVolumeUSD', ZERO_DECIMAL128).to_decimal(),
            volume=data.get('volume', ZERO_DECIMAL128).to_decimal(),
            volumeUSD=data.get('volumeUSD', ZERO_DECIMAL128).to_decimal(),
            txCount=data.get('txCount', 0),
        )


async def get_tokens(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForToken] = None
) -> List[Token]:
    db: Database = info.context['db']
    query = {}

    if where is not None:
        if where.token_address is not None:
            query['tokenAddress'] = where.token_address
        if where.token_address_in:
            token_in = [token for token in where.token_address_in]
            query['tokenAddress'] = {'$in': token_in}

    cursor = db[Collection.TOKENS].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [Token.from_mongo(d) for d in cursor]


async def get_token(db: Database, id: str) -> Token:
    query = {'tokenAddress': id}
    token = db['tokens'].find_one(query)
    return Token.from_mongo(token)
