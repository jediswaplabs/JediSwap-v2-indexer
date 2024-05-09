from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import add_order_by_constraint
from server.const import Collection
from server.query_utils import filter_by_the_latest_value
from server.utils import amount_after_decimals
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
            depositedToken0=amount_after_decimals(data.get('depositedToken0', 0), data.get('token0Decimals', DEFAULT_DECIMALS)),
            depositedToken1=amount_after_decimals(data.get('depositedToken1', 0), data.get('token1Decimals', DEFAULT_DECIMALS)),
            withdrawnToken0=amount_after_decimals(data.get('withdrawnToken0', 0), data.get('token0Decimals', DEFAULT_DECIMALS)),
            withdrawnToken1=amount_after_decimals(data.get('withdrawnToken1', 0), data.get('token1Decimals', DEFAULT_DECIMALS)),
            liquidity=Decimal(data.get('liquidity', 0)),
            datetime=convert_timestamp_to_datetime(data['timestamp']),
            block=data['block'],
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
    if where is not None:
        if where.position_id is not None:
            query['positionId'] = where.position_id
        if where.owner_address is not None:
            query['ownerAddress'] = where.owner_address

    cursor = db[Collection.POSITIONS].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [NftPosition.from_mongo(d) for d in cursor]
