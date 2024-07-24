from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import add_order_by_constraint, WhereFilterForUser, filter_by_user_address
from server.const import Collection


@strawberry.type
class VolumeLeaderboardSnapshot:
    userAddress: str
    swapFeesUsd: Decimal
    timestamp: float
    sybilMultiplier: int
    earlyMultiplier: int
    volumePoints: Decimal
    processed: bool

    @classmethod
    def from_mongo(cls, data):
        return cls(
            userAddress=data['userAddress'],
            swapFeesUsd=data['swapFeesUsd'].to_decimal(),
            timestamp=data['timestamp'],
            sybilMultiplier=data['sybilMultiplier'],
            earlyMultiplier=data['earlyMultiplier'],
            volumePoints=data['volumePoints'].to_decimal(),
            processed=data['processed'],
        )


async def get_volume_leaderboard_snapshot(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForUser] = None
) -> List[VolumeLeaderboardSnapshot]:
    db: Database = info.context['db']
    query = {}
    if where is not None:
        await filter_by_user_address(where, query)

    cursor = db[Collection.VOLUME_LEADERBOARD_SNAPSHOT].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)
    return [VolumeLeaderboardSnapshot.from_mongo(d) for d in cursor]
