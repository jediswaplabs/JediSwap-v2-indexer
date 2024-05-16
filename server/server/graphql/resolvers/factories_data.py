from typing import List

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import (
    filter_by_days_data,
    PERIODS_TO_DAYS_MAP
)
from server.const import Collection


@strawberry.type
class FactoriesData:
    one_day: strawberry.scalars.JSON = strawberry.field(name="one_day")
    two_days: strawberry.scalars.JSON = strawberry.field(name="two_days")
    one_week: strawberry.scalars.JSON = strawberry.field(name="one_week")
    one_month: strawberry.scalars.JSON = strawberry.field(name="one_month")

    @classmethod
    def from_mongo(cls, data):
        return cls(
            one_day=data['one_day'],
            two_days=data['two_days'],
            one_week=data['one_week'],
            one_month=data['one_month'],
        )


async def get_factories_data(info: Info) -> List[FactoriesData]:
    db: Database = info.context['db']

    total_value_locked_usd = None
    factories_period_data = {}
    for period_name in PERIODS_TO_DAYS_MAP:
        period_query = {}
        await filter_by_days_data(period_query, period_name)
        pipeline = [
            {
                "$match": period_query
            },
            {
                "$group": {
                    "_id": None,
                    "feesUSD": {"$sum": "$feesUSD"},
                    "totalValueLockedUSD": {"$last": "$totalValueLockedUSD"},
                    "totalValueLockedUSDFirst": {"$first": "$totalValueLockedUSD"},
                    "volumeETH": {"$sum": "$volumeETH"},
                    "volumeUSD": {"$sum": "$volumeUSD"},
                    "txCount": {"$sum": "$txCount"},
                }
            }
        ]
        cursor = db[Collection.FACTORIES_HOUR_DATA].aggregate(pipeline)
        
        try:
            record = next(cursor)
            total_value_locked_usd = str(record['totalValueLockedUSD'].to_decimal())
            factories_period_data[period_name] = {
                'feesUSD': str(record['feesUSD'].to_decimal()),
                'totalValueLockedUSD': total_value_locked_usd,
                'totalValueLockedUSDFirst': str(record['totalValueLockedUSDFirst'].to_decimal()),
                'volumeETH': str(record['volumeETH'].to_decimal()),
                'volumeUSD': str(record['volumeUSD'].to_decimal()),
                'txCount': str(record['txCount']),
            }
        except StopIteration:
            if total_value_locked_usd is None:
                factory_record = next(db[Collection.FACTORIES].find({}))
                total_value_locked_usd = str(factory_record['totalValueLockedUSD'].to_decimal())

            factories_period_data[period_name] = {
                'feesUSD': '0',
                'totalValueLockedUSD': total_value_locked_usd,
                'totalValueLockedUSDFirst': '0',
                'volumeETH': '0',
                'volumeUSD': '0',
                'txCount': '0',
            }

    return [FactoriesData.from_mongo(factories_period_data)]
