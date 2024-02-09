from dataclasses import field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, TypeVar, Generic, List

from bson import Decimal128
from pymongo import ASCENDING, DESCENDING
from pymongo.cursor import CursorType
from pymongo.database import Database

import strawberry

from server.const import Collection
from server.utils import format_address


PERIODS_TO_DAYS_MAP = {
    'one_day': 1,
    'two_days': 2,
    'one_week': 7,
    'one_month': 30,
}


@strawberry.input
class BlockFilter:
    number: Optional[int]


@strawberry.input
class WhereFilterForToken:
    token_address: Optional[str] = None
    token_address_in: Optional[List[str]] = field(default_factory=list)


@strawberry.input
class WhereFilterForPool:
    pool_address: Optional[str] = None
    pool_address_in: Optional[List[str]] = field(default_factory=list)
    token0_address: Optional[str] = None
    token1_address: Optional[str] = None


@strawberry.input
class WhereFilterForPoolData:
    pool_address: Optional[str] = None
    pool_address_in: Optional[List[str]] = field(default_factory=list)
    both_token_address_in: Optional[List[str]] = field(default_factory=list)

@strawberry.input
class WhereFilterForTransaction:
    pool_address: Optional[str] = None
    pool_address_in: Optional[List[str]] = field(default_factory=list)
    tx_type_in: Optional[List[str]] = field(default_factory=lambda: ['Swap', 'Burn', 'Mint'])
    tx_sender: Optional[str] = None


async def add_block_constraint(query: dict, block: Optional[BlockFilter]):
    if block and block.number:
        query["$or"] = [
            {
                "$and": [
                    {"_cursor.to": None},
                    {"_cursor.from": {"$lte": block.number}},
                ]
            },
            {
                "$and": [
                    {"_cursor.to": {"$gt": block.number}},
                    {"_cursor.from": {"$lte": block.number}},
                ]
            },
        ]


async def add_order_by_constraint(cursor: CursorType, orderBy: Optional[str] = None, 
                            orderByDirection: Optional[str] = 'asc') -> CursorType:
    if orderBy:
        if orderByDirection == 'asc':
            cursor = cursor.sort(orderBy, ASCENDING)
        else:
            cursor = cursor.sort(orderBy, DESCENDING)
    return cursor


async def filter_by_token_address(where: WhereFilterForToken, query: dict):
    if where.token_address is not None:
        query['tokenAddress'] = format_address(where.token_address)
    if where.token_address_in:
        token_in = [format_address(token) for token in where.token_address_in]
        query['tokenAddress'] = {'$in': token_in}


async def filter_by_pool_address(where: WhereFilterForPoolData, query: dict):
    if where.pool_address is not None:
        query['poolAddress'] = format_address(where.pool_address)
    if where.pool_address_in:
        pool_in = [format_address(pool) for pool in where.pool_address_in]
        query['poolAddress'] = {'$in': pool_in}


async def filter_pools_by_token_addresses(where: WhereFilterForPoolData, query: dict, db: Database):
    if where.both_token_address_in:
        tokens_in = [format_address(token) for token in where.both_token_address_in]
        pool_query = {"token0": {"$in": tokens_in}, "token1": {"$in": tokens_in}}
        cursor = db[Collection.POOLS].find(pool_query)
        pool_addresses = [d["poolAddress"] for d in cursor]
        if where.pool_address_in:
            where.pool_address_in = [pool_address for pool_address in pool_addresses if pool_address in where.pool_address_in]
        else:
            where.pool_address_in.extend(pool_addresses)


async def filter_pools(where: WhereFilterForPool, query: dict):
    if where.pool_address is not None:
        query['poolAddress'] = format_address(where.pool_address)
    if where.pool_address_in:
        pool_in = [format_address(pool) for pool in where.pool_address_in]
        query['poolAddress'] = {'$in': pool_in}
    if where.token0_address is not None:
        query["token0"] = format_address(where.token0_address)
    if where.token1_address is not None:
        query["token1"] = format_address(where.token1_address)

async def filter_transactions(where: WhereFilterForTransaction, query: dict):
    if where.pool_address is not None:
        query['poolAddress'] = format_address(where.pool_address)
    if where.pool_address_in:
        pool_in = [format_address(pool) for pool in where.pool_address_in]
        query['poolAddress'] = {'$in': pool_in}
    if where.tx_type_in:
        query['event'] = {'$in': where.tx_type_in}
    if where.tx_sender is not None:
        query['tx_sender'] = format_address(where.tx_sender)

async def filter_by_days_data(query: dict, period: str):
    days = PERIODS_TO_DAYS_MAP.get(period)
    from_datestamp = datetime.now() - timedelta(days=days)
    query['periodStartUnix'] = {'$gt': from_datestamp}


def convert_timestamp_to_datetime(timestamp: float):
    return datetime.fromtimestamp(timestamp / 1e3)


async def validate_period_input(where) -> list[str]:
    periods = getattr(where, 'period_in', list(PERIODS_TO_DAYS_MAP))
    periods = periods or list(PERIODS_TO_DAYS_MAP)
    for period in periods:
        if not PERIODS_TO_DAYS_MAP.get(period):
            raise TypeError(f'Invalid period value. Allowed values: {list(PERIODS_TO_DAYS_MAP)}')
    return periods
