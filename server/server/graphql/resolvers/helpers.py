from dataclasses import field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, TypeVar, Generic, List

from bson import Decimal128
from pymongo import ASCENDING, DESCENDING
from pymongo.cursor import CursorType

import strawberry

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


def add_block_constraint(query: dict, block: Optional[BlockFilter]):
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


def add_order_by_constraint(cursor: CursorType, orderBy: Optional[str] = None, 
                            orderByDirection: Optional[str] = 'asc') -> CursorType:
    if orderBy:
        if orderByDirection == 'asc':
            cursor = cursor.sort(orderBy, ASCENDING)
        else:
            cursor = cursor.sort(orderBy, DESCENDING)
    return cursor


def filter_by_token_address(where: WhereFilterForToken, query: dict):
    if where.token_address is not None:
        query['tokenAddress'] = format_address(where.token_address)
    if where.token_address_in:
        token_in = [format_address(token) for token in where.token_address_in]
        query['tokenAddress'] = {'$in': token_in}


def filter_by_pool_address(where: WhereFilterForPoolData, query: dict):
    if where.pool_address is not None:
        query['poolAddress'] = format_address(where.pool_address)
    if where.pool_address_in:
        pool_in = [format_address(pool) for pool in where.pool_address_in]
        query['poolAddress'] = {'$in': pool_in}


def filter_pools(where: WhereFilterForPool, query: dict):
    if where.pool_address is not None:
        query['poolAddress'] = format_address(where.pool_address)
    if where.pool_address_in:
        pool_in = [format_address(pool) for pool in where.pool_address_in]
        query['poolAddress'] = {'$in': pool_in}
    if where.token0_address is not None:
        query["token0"] = format_address(where.token0_address)
    if where.token1_address is not None:
        query["token1"] = format_address(where.token1_address)


def filter_by_days_data(query: dict, period: str):
    days = PERIODS_TO_DAYS_MAP.get(period)
    from_datestamp = datetime.now() - timedelta(days=days)
    query['periodStartUnix'] = {'$gt': from_datestamp}


def convert_timestamp_to_datetime(timestamp: float):
    return datetime.fromtimestamp(timestamp / 1e3)


def get_liquidity_value(data: dict) -> Decimal:
    return data['liquidity'].to_decimal() if isinstance(data['liquidity'], Decimal128) else Decimal(data['liquidity'])


def validate_period_input(where) -> list[str]:
    periods = getattr(where, 'period_in', list(PERIODS_TO_DAYS_MAP))
    periods = periods or list(PERIODS_TO_DAYS_MAP)
    for period in periods:
        if not PERIODS_TO_DAYS_MAP.get(period):
            raise TypeError(f'Invalid period value. Allowed values: {list(PERIODS_TO_DAYS_MAP)}')
    return periods
