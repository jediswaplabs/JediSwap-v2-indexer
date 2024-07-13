from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import add_order_by_constraint, WhereFilterForNftPosition
from server.const import Collection, ZERO_DECIMAL128
from server.utils import format_address
from server.graphql.resolvers.tokens import Token


@strawberry.type
class NftPosition:
    positionId: int

    positionAddress: str
    ownerAddress: str
    liquidity: Decimal

    depositedToken0: Decimal
    depositedToken1: Decimal
    withdrawnToken0: Decimal
    withdrawnToken1: Decimal
    collectedFeesToken0: Decimal
    collectedFeesToken1: Decimal

    token0Address: strawberry.Private[str]
    token1Address: strawberry.Private[str]

    @strawberry.field
    def token0(self, info: Info) -> Token:
        return info.context["token_loader"].load(self.token0Address)

    @strawberry.field
    def token1(self, info: Info) -> Token:
        return info.context["token_loader"].load(self.token1Address)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            positionId=data['positionId'],
            positionAddress=data['positionAddress'],
            ownerAddress=data['ownerAddress'],
            token0Address=data['token0Address'],
            token1Address=data['token1Address'],
            depositedToken0=data.get('depositedToken0', 0),
            depositedToken1=data.get('depositedToken1', 0),
            withdrawnToken0=data.get('withdrawnToken0', 0),
            withdrawnToken1=data.get('withdrawnToken1', 0),
            liquidity=data.get('liquidity', ZERO_DECIMAL128).to_decimal(),
            collectedFeesToken0=data['collectedFeesToken0'].to_decimal(),
            collectedFeesToken1=data['collectedFeesToken1'].to_decimal(),
        )


async def get_nft_positions(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForNftPosition] = None
) -> List[NftPosition]:
    db: Database = info.context['db']

    query = {}
    if where is not None:
        if where.position_id is not None:
            query['positionId'] = where.position_id
        if where.owner_address is not None:
            query['ownerAddress'] = format_address(where.owner_address)
        if where.pool_address is not None:
            query['position.poolAddress'] = format_address(where.pool_address)

    cursor = db[Collection.POSITIONS].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [NftPosition.from_mongo(d) for d in cursor]
