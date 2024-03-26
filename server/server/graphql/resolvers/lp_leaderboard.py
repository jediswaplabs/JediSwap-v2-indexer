from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import add_order_by_constraint, WhereFilterForUser, filter_by_user_address
from server.const import Collection


@strawberry.type
class LpLeaderboard:
    userAddress: str
    points: Decimal

    @classmethod
    def from_mongo(cls, data):
        return cls(
            userAddress=data['userAddress'],
            points=data['points'].to_decimal(),
        )


async def get_lp_leaderboard_points(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForUser] = None
) -> List[LpLeaderboard]:
    db: Database = info.context['db']
    query = {}
    if where is not None:
        await filter_by_user_address(where, query)
    cursor = db[Collection.LP_LEADERBOARD].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)
    return [LpLeaderboard.from_mongo(d) for d in cursor]
