import datetime as dt
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info

from server.graphql.resolvers.helpers import add_order_by_constraint, convert_timestamp_to_datetime, WhereFilterForTransaction, filter_transactions
from server.const import Collection
from server.graphql.resolvers.pools import Pool


@strawberry.type
class Transaction:
    txHash: str
    txType: str
    txSender: str

    amount0: Decimal
    amount1: Decimal

    datetime: dt.datetime

    poolAddress: strawberry.Private[str]
    @strawberry.field
    def pool(self, info: Info) -> Pool:
        return info.context["pool_loader"].load(self.poolAddress)

    @classmethod
    def from_mongo(cls, data):
        return cls(
            txHash=data['tx_hash'],
            txType=data['event'],
            txSender=data['tx_sender'],
            datetime=convert_timestamp_to_datetime(data['timestamp']),
            amount0=Decimal(data['amount0']),
            amount1=Decimal(data['amount1']),
            poolAddress=data['poolAddress'],
        )


async def get_transactions(
    info: Info, first: Optional[int] = 100, skip: Optional[int] = 0, orderBy: Optional[str] = None, 
    orderByDirection: Optional[str] = 'asc', where: Optional[WhereFilterForTransaction] = None
) -> List[Transaction]:
    db: Database = info.context['db']
    query = {"processed": True}

    await filter_transactions(where, query)

    cursor = db[Collection.POOLS_DATA].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [Transaction.from_mongo(d) for d in cursor]
