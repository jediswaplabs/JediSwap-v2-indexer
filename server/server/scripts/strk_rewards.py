from collections import defaultdict
import csv
import json
import math
from pathlib import Path

from pymongo import MongoClient

from server.const import Collection, ETH, USDC, USDT, WBTC, STRK

from structlog import get_logger


logger = get_logger(__name__)

OUTPUT_DIR = Path('strk_rewards_data')
USER_ALLOCATION_FILE = OUTPUT_DIR / 'user_allocations'
USER_ALLOCATION_PAIR_FILE = OUTPUT_DIR / 'user_allocations_{}'

ALLOCATION_START_TIMESTAMP = 1715105005000
ALLOCATION_END_TIMESTAMP = 1735105005000
TOKENS_TO_ALLOCATION_MAP = {
    (USDC, ETH): 205399 * (10 ** 16),
    (STRK, ETH): 176640 * (10 ** 16),
    (STRK, USDC): 10267 * (10 ** 16),
    (USDC, USDT): 10267 * (10 ** 16),
    (WBTC, ETH): 10267 * (10 ** 16),
}


async def get_obj_name(token_address: str):
    for name, value in globals().items():
        if value == token_address:
            return name


async def save_to_yaml(allocations_array: list, allocation_file: Path):
    allocations_array_json = json.dumps(allocations_array)
    with open(allocation_file.with_suffix('.json'), 'w') as output_file:
        output_file.write(allocations_array_json)


async def save_to_csv(allocations_array: list, allocation_file: Path):
    with open(allocation_file.with_suffix('.csv'), 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, allocations_array[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(allocations_array)


async def strk_rewards_calculation(mongo_url: str, mongo_database: str):
    with MongoClient(mongo_url) as mongo:
        db_name = mongo_database.replace('-', '_')
        db = mongo[db_name]

        match_query = {
            'lpPoints': {'$ne': 0},
            'timestamp': {
                '$lte': ALLOCATION_END_TIMESTAMP,
                '$gte': ALLOCATION_START_TIMESTAMP,
            }
        }
        total_points_pipeline = [
            {
                '$match': match_query,
            },
            {
                '$group': {
                    '_id': {
                        'token0Address': '$position.token0Address',
                        'token1Address': '$position.token1Address'
                    },
                    'totalPoints': {'$sum': '$lpPoints'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'token0Address': '$_id.token0Address',
                    'token1Address': '$_id.token1Address',
                    'totalPoints': '$totalPoints',
                }
            }
        ]

        total_points_result = db[Collection.LP_LEADERBOARD_SNAPSHOT].aggregate(total_points_pipeline)

        user_points_pipeline = [
            {
                '$match': match_query
            },
            {
                '$group': {
                    '_id': {
                        'ownerAddress': '$position.ownerAddress',
                        'token0Address': '$position.token0Address',
                        'token1Address': '$position.token1Address'
                    },
                    'totalPoints': {'$sum': '$lpPoints'}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'userAddress': '$_id.ownerAddress',
                    'token0Address': '$_id.token0Address',
                    'token1Address': '$_id.token1Address',
                    'totalPoints': '$totalPoints',
                }
            }
        ]

        user_points_result = db[Collection.LP_LEADERBOARD_SNAPSHOT].aggregate(user_points_pipeline)

        pairs_total_points = {
            (pair['token0Address'], pair['token1Address']): pair['totalPoints'].to_decimal()
            for pair in total_points_result
        }

        total_user_allocations = defaultdict(int)
        pair_user_allocations = {}
        for user_record in user_points_result:
            user_pair_tokens = {user_record['token0Address'], user_record['token1Address']}
            for pair_tokens, allocation_reward in TOKENS_TO_ALLOCATION_MAP.items():
                if set(pair_tokens) == user_pair_tokens:
                    pair_rewards = allocation_reward
                    pair_total_points = pairs_total_points.get(
                        (user_record['token0Address'], user_record['token1Address']),
                        (user_record['token1Address'], user_record['token0Address'])
                    )
                    break
            else:
                continue

            token_0_name = await get_obj_name(user_record['token0Address'])
            token_1_name = await get_obj_name(user_record['token1Address'])
            sorted_pair_tokens = '_'.join(sorted([token_0_name, token_1_name]))
            if sorted_pair_tokens not in pair_user_allocations:
                pair_user_allocations[sorted_pair_tokens] = defaultdict(int)
            
            amount = math.floor(user_record['totalPoints'].to_decimal() * pair_rewards / pair_total_points)
            total_user_allocations[user_record['userAddress']] += amount
            pair_user_allocations[sorted_pair_tokens][user_record['userAddress']] += amount

        total_user_allocations_array = [
            {
                'address': user,
                'amount': total_user_allocations[user],
            } for user in total_user_allocations.keys()
        ]

        await save_to_yaml(total_user_allocations_array, USER_ALLOCATION_FILE)
        await save_to_csv(total_user_allocations_array, USER_ALLOCATION_FILE)

        logger.info(f'Total STRK rewards successfully calculated in {USER_ALLOCATION_FILE}')

        for pair_name, pair_data in pair_user_allocations.items():
            pair_user_allocations_array = [
                {
                    'address': user,
                    'amount': pair_data[user],
                } for user in pair_data.keys()
            ]
            
            user_allocation_pair_file = Path(str(USER_ALLOCATION_PAIR_FILE).format(pair_name))
            await save_to_yaml(pair_user_allocations_array, user_allocation_pair_file)
            await save_to_csv(pair_user_allocations_array, user_allocation_pair_file)

            logger.info(f'"{pair_name}" STRK rewards successfully calculated in {user_allocation_pair_file}')
