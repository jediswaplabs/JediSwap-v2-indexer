from bson import Decimal128
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import add_order_by_constraint, WhereFilterForNftPosition, format_address
from server.const import Collection


@strawberry.type
class LpLeaderboardSnapshot:
    positionId: int
    ownerAddress: str
    liquidity: Decimal
    timestamp: float
    block: int
    event: str
    currentFeesUsd: Decimal
    lpPoints: Decimal
    processed: bool
    timeVestedValue: Decimal
    newTimeVestedValue: Decimal
    poolBoost: Decimal
    period: float

    @classmethod
    def from_mongo(cls, data):
        liquidity = data['liquidity'].to_decimal() if type(data['liquidity']) is Decimal128 else Decimal(data['liquidity'])
        event = data['event'] or ''
        return cls(
            positionId=data['positionId'],
            ownerAddress=data['position']['ownerAddress'],
            liquidity=liquidity,
            timestamp=data['timestamp'],
            block=data['block'],
            event=event,
            currentFeesUsd=data['currentFeesUsd'].to_decimal(),
            lpPoints=data['lpPoints'].to_decimal(),
            processed=data['processed'],
            timeVestedValue=data['timeVestedValue'].to_decimal(),
            newTimeVestedValue=data['newTimeVestedValue'].to_decimal(),
            poolBoost=data['poolBoost'].to_decimal(),
            period=data['period'],
        )


async def get_lp_leaderboard_snapshot(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForNftPosition] = None
) -> List[LpLeaderboardSnapshot]:
    db: Database = info.context['db']
    query = {}
    if where is not None:
        if where.position_id is not None:
            query['positionId'] = where.position_id
        if where.owner_address is not None:
            query['position.ownerAddress'] = format_address(where.owner_address)

    cursor = db[Collection.LP_LEADERBOARD_SNAPSHOT].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)
    return [LpLeaderboardSnapshot.from_mongo(d) for d in cursor]
