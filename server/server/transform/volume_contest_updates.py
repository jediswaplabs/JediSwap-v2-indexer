from datetime import timedelta
from decimal import Decimal

from bson import Decimal128
from pymongo import UpdateOne
from pymongo.database import Database

from server.const import Collection
from server.graphql.resolvers.helpers import add_order_by_constraint
from server.graphql.resolvers.helpers import convert_timestamp_to_datetime

from structlog import get_logger


logger = get_logger(__name__)


EARLY_ADOPTER_MULTIPLIER_CONST = 3
LAST_SWAPS_PERIOD_IN_HOURS = 24


async def calculate_volume_leaderboard_points(db: Database, fees_USD: Decimal, record: dict):
    dt = convert_timestamp_to_datetime(record['timestamp'])
    dt = dt - timedelta(hours=LAST_SWAPS_PERIOD_IN_HOURS)
    timestamp_period = int(dt.timestamp() * 1000)

    query = {
        'timestamp': {
            '$gte': timestamp_period
        }
    }
    cursor = db[Collection.VOLUME_LEADERBOARD_SNAPSHOT].find(query)
    cursor = await add_order_by_constraint(cursor, 'swapFeesUsd')
    records = list(cursor)

    sybil_multiplier = 1
    if records:
        last_index = int(len(records) * 0.75) - 1
        last_swap_event = records[last_index]
        if last_swap_event['swapFeesUsd'].to_decimal() > fees_USD:
            sybil_multiplier = 0

    points = fees_USD * EARLY_ADOPTER_MULTIPLIER_CONST * sybil_multiplier * 1000

    volume_leaderboard_snapshot_record = {
        'userAddress': record['sender'],
        'swapFeesUsd': Decimal128(fees_USD),
        'sybilMultiplier': sybil_multiplier,
        'earlyMultiplier': EARLY_ADOPTER_MULTIPLIER_CONST,
        'timestamp': record['timestamp'],
        'volumePoints': Decimal128(points),
    }
    db[Collection.VOLUME_LEADERBOARD_SNAPSHOT].insert_one(volume_leaderboard_snapshot_record)

    volume_contest_update_operation = [
        UpdateOne({
            'userAddress': record['sender'],
        },{
            '$inc': {
                'points': Decimal128(points),
            }
        }, upsert=True)
    ]
    db[Collection.VOLUME_LEADERBOARD].bulk_write(volume_contest_update_operation)
