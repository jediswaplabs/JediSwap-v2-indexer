import datetime as dt
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import add_order_by_constraint, convert_timestamp_to_datetime
from server.const import Collection, DEFAULT_DECIMALS
from server.query_utils import filter_by_the_latest_value
from server.utils import amount_after_decimals


@strawberry.type
class NftPositionFee:
    positionId: int

    positionAddress: str
    ownerAddress: str
    collectedFeesToken0: Decimal
    collectedFeesToken1: Decimal
    datetime: dt.datetime
    block: int

    @classmethod
    def from_mongo(cls, data):
        return cls(
            positionId=data['positionId'],
            positionAddress=data['positionAddress'],
            ownerAddress=data['ownerAddress'],
            collectedFeesToken0=amount_after_decimals(data.get('collectedFeesToken0', 0), data.get('token0Decimals', DEFAULT_DECIMALS)),
            collectedFeesToken1=amount_after_decimals(data.get('collectedFeesToken1', 0), data.get('token1Decimals', DEFAULT_DECIMALS)),
            datetime=convert_timestamp_to_datetime(data['timestamp']),
            block=data['block'],
        )


@strawberry.input
class WhereFilterForNftPositionFee:
    position_id: Optional[int] = None
    owner_address: Optional[str] = None



async def get_nft_position_fees(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForNftPositionFee] = None
) -> List[NftPositionFee]:
    db: Database = info.context['db']

    query = {}
    filter_by_the_latest_value(query)
    if where is not None:
        if where.position_id is not None:
            query['positionId'] = where.position_id
        if where.owner_address is not None:
            query['ownerAddress'] = where.owner_address

    cursor = db[Collection.POSITION_FEES].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [NftPositionFee.from_mongo(d) for d in cursor]
