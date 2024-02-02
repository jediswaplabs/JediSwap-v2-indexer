from dataclasses import field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, TypeVar, Generic, List

from bson import Decimal128
from pymongo import ASCENDING, DESCENDING
from pymongo.cursor import CursorType

import strawberry


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


T = TypeVar('T')


@strawberry.input
class EQFilter(Generic[T]):
    eq: Optional[T] = None


@strawberry.input
class GTLTFilter(Generic[T]):
    gt: Optional[T] = None
    lt: Optional[T] = None


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
        query['tokenAddress'] = where.token_address
    if where.token_address_in:
        token_in = [token for token in where.token_address_in]
        query['tokenAddress'] = {'$in': token_in}


def filter_by_pool_address(where: WhereFilterForPool, query: dict):
    if where.pool_address is not None:
        query['poolAddress'] = where.pool_address
    if where.pool_address_in:
        token_in = [token for token in where.pool_address_in]
        query['poolAddress'] = {'$in': token_in}


def filter_by_last_seven_days(query: dict):
    from_datestamp = datetime.now() - timedelta(days=7)
    from_timestamp = int(from_datestamp.timestamp() * 1000)
    query['date'] = {'$gt': from_timestamp}


def convert_timestamp_to_datetime(timestamp: float):
    return datetime.fromtimestamp(timestamp / 1e3)


def get_liquidity_value(data: dict) -> Decimal:
    return data['liquidity'].to_decimal() if isinstance(data['liquidity'], Decimal128) else Decimal(data['liquidity'])
