from decimal import Decimal
from typing import List, Optional
from dataclasses import field

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.resolvers.helpers import add_order_by_constraint
from const import Collection


@strawberry.type
class Token:
    address: str

    name: str
    symbol: str
    decimals: int

    totalValueLocked: Decimal

    @classmethod
    def from_mongo(cls, data):
        return cls(
            address=data['address'],
            name=data['name'],
            symbol=data['symbol'],
            decimals=data['decimals'],
            totalValueLocked=data['totalValueLocked'].to_decimal(),
        )


@strawberry.input
class WhereFilterForToken:
    address: Optional[str] = None
    address_in: Optional[List[str]] = field(default_factory=list)


async def get_tokens(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForToken] = None
) -> List[Token]:
    db: Database = info.context['db']

    query = {}

    if where is not None:
        if where.address is not None:
            query['address'] = where.address
        if where.address_in:
            token_in = [token for token in where.address_in]
            query['address'] = {'$in': token_in}

    cursor = db[Collection.TOKENS].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [Token.from_mongo(d) for d in cursor]
