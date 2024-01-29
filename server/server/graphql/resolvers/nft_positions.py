from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import add_order_by_constraint, convert_timestamp_to_datetime
from server.const import Collection, DEFAULT_DECIMALS
from server.query_utils import filter_by_the_latest_value
from server.utils import to_decimal


@strawberry.type
class NftPosition:
    positionId: int

    positionAddress: str
    ownerAddress: str
    depositedToken0: Decimal
    depositedToken1: Decimal
    withdrawnToken0: Decimal
    withdrawnToken1: Decimal
    liquidity: Decimal
    timestamp: str
    block: int

    @classmethod
    def from_mongo(cls, data):
        return cls(
            positionId=data['positionId'],
            positionAddress=data['positionAddress'],
            ownerAddress=data['ownerAddress'],
            depositedToken0=to_decimal(data.get('depositedToken0', 0), data.get('token0Decimals', DEFAULT_DECIMALS)),
            depositedToken1=to_decimal(data.get('depositedToken1', 0), data.get('token1Decimals', DEFAULT_DECIMALS)),
            withdrawnToken0=to_decimal(data.get('withdrawnToken0', 0), data.get('token0Decimals', DEFAULT_DECIMALS)),
            withdrawnToken1=to_decimal(data.get('withdrawnToken1', 0), data.get('token1Decimals', DEFAULT_DECIMALS)),
            liquidity=Decimal(data.get('liquidity', 0)),
            timestamp=convert_timestamp_to_datetime(data['timestamp']),
            block=data['block'],
        )


@strawberry.input
class WhereFilterForNftPosition:
    position_id: Optional[int] = None
    owner_address: Optional[str] = None



def get_nft_positions(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForNftPosition] = None
) -> List[NftPosition]:
    db: Database = info.context['db']

    query = {}
    filter_by_the_latest_value(query)
    if where is not None:
        if where.position_id is not None:
            query['positionId'] = where.position_id
        if where.owner_address is not None:
            query['ownerAddress'] = where.owner_address

    cursor = db[Collection.POSITIONS].find(query, skip=skip, limit=first)
    cursor = add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [NftPosition.from_mongo(d) for d in cursor]
