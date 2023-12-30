from typing import Optional, TypeVar, Generic
from pymongo import ASCENDING, DESCENDING
from pymongo.cursor import CursorType

import strawberry


@strawberry.input
class BlockFilter:
    number: Optional[int]


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
