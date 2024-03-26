from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import add_order_by_constraint
from server.const import Collection
from server.query_utils import filter_by_the_latest_value


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


    @classmethod
    def from_mongo(cls, data):
        return cls(
            positionId=data['positionId'],
            positionAddress=data['positionAddress'],
            ownerAddress=data['ownerAddress'],
            liquidity=Decimal(data['liquidity']),
            depositedToken0=data['depositedToken0'].to_decimal(),
            depositedToken1=data['depositedToken1'].to_decimal(),
            withdrawnToken0=data['withdrawnToken0'].to_decimal(),
            withdrawnToken1=data['withdrawnToken1'].to_decimal(),
            collectedFeesToken0=data['collectedFeesToken0'].to_decimal(),
            collectedFeesToken1=data['collectedFeesToken1'].to_decimal(),
        )


@strawberry.input
class WhereFilterForNftPosition:
    position_id: Optional[int] = None
    owner_address: Optional[str] = None



async def get_nft_positions(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForNftPosition] = None
) -> List[NftPosition]:
    db: Database = info.context['db']

    query = {}
    await filter_by_the_latest_value(query)
    if where is not None:
        if where.position_id is not None:
            query['positionId'] = where.position_id
        if where.owner_address is not None:
            query['ownerAddress'] = where.owner_address

    cursor = db[Collection.POSITIONS].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [NftPosition.from_mongo(d) for d in cursor]
