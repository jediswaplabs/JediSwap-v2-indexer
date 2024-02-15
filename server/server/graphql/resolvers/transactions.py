import datetime as dt
from decimal import Decimal
from typing import List, Optional

import strawberry
from pymongo.database import Database
from strawberry.types import Info
from strawberry.scalars import JSON

from server.graphql.resolvers.helpers import add_order_by_constraint, convert_timestamp_to_datetime, WhereFilterForTransaction, filter_transactions, filter_pools_by_token_addresses
from server.const import Collection
from server.graphql.resolvers.pools import Pool


@strawberry.type
class Transaction:
    txHash: str
    txType: str
    txSender: str

    amount0: Decimal
    amount1: Decimal

    timestamp: strawberry.Private[str]
    datetime: dt.datetime

    poolAddress: strawberry.Private[str]
    
    @strawberry.field
    def pool(self, info: Info) -> Pool:
        return info.context["pool_loader"].load(self.poolAddress)
    
    @strawberry.field
    def pricesUSD(self, info: Info) -> JSON:
        return info.context["transaction_value_loader"].load((self.poolAddress, self.timestamp, self.txType, self.amount0, self.amount1))

    @classmethod
    def from_mongo(cls, data):
        return cls(
            txHash=data['tx_hash'],
            txType=data['event'],
            txSender=data['tx_sender'],
            timestamp=data['timestamp'],
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

    if where is not None:
        await filter_pools_by_token_addresses(where, query, db)
        await filter_transactions(where, query)

    cursor = db[Collection.POOLS_DATA].find(query, skip=skip, limit=first)
    cursor = await add_order_by_constraint(cursor, orderBy, orderByDirection)

    return [Transaction.from_mongo(d) for d in cursor]
