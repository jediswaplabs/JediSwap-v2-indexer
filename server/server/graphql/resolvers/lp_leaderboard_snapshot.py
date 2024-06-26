from bson import Decimal128
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info
import pytz

from server.graphql.resolvers.helpers import (
    add_order_by_constraint, WhereFilterForNftPosition, format_address, convert_timestamp_to_datetime
)
from server.const import Collection


@strawberry.type
class LpLeaderboardSnapshot:
    positionId: str
    vaultAddress: str
    ownerAddress: str
    poolAddress: str
    liquidity: Decimal
    calculationAt: str
    block: int
    event: str
    currentFeesUsd: Decimal
    lpPoints: Decimal
    processed: bool
    lastTimeVestedValue: Decimal
    currentTimeVestedValue: Decimal
    poolBoost: Decimal
    period: float

    @classmethod
    def from_mongo(cls, data):
        liquidity = data['liquidity'].to_decimal() if type(data['liquidity']) is Decimal128 else Decimal(data['liquidity'])
        event = data['event'] or ''
        calculation_at_dt = convert_timestamp_to_datetime(data['timestamp']).astimezone(pytz.utc)
        calculation_at = calculation_at_dt.strftime("%Y-%m-%d %H:%M:%S %Z %z")
        return cls(
            positionId=str(data['positionId']),
            vaultAddress=data['position'].get('vaultAddress', ''),
            ownerAddress=data['position']['ownerAddress'],
            poolAddress=data['position'].get('poolAddress', ''),
            liquidity=liquidity,
            calculationAt=calculation_at,
            block=data['block'],
            event=event,
            currentFeesUsd=data['currentFeesUsd'].to_decimal(),
            lpPoints=data['lpPoints'].to_decimal(),
            processed=data['processed'],
            lastTimeVestedValue=data['lastTimeVestedValue'].to_decimal(),
            currentTimeVestedValue=data['currentTimeVestedValue'].to_decimal(),
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
        if where.pool_address is not None:
            query['position.poolAddress'] = format_address(where.pool_address)

    cursor = db[Collection.LP_LEADERBOARD_SNAPSHOT].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)
    return [LpLeaderboardSnapshot.from_mongo(d) for d in cursor]
