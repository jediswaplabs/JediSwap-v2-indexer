import asyncio
from functools import wraps

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv


ENV_FILE = Path(__file__).parent.parent.parent / 'env_sepolia'

def async_command(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))

    return wrapper


@async_command
async def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['events', 'positions', 'leaderboard', 'graphql', 'strk-calculation'], 
                        help='Choose an action')
    parser.add_argument('--env-file', help='Run with mainnet config')

    args = parser.parse_args()
    
    env_file = args.env_file if args.env_file else ENV_FILE
    load_dotenv(env_file)

    ## Load after dotenv
    from server.graphql.main import run_graphql_server
    from server.transform.events_transformer import run_events_transformer
    from server.transform.leaderboard_transformer import run_leaderboard_transformer
    from server.transform.positions_transformer import run_positions_transformer
    from server.scripts.strk_rewards import strk_rewards_calculation

    mongo_url = os.environ.get('MONGODB_CONNECTION_STRING', None)
    if mongo_url is None:
        sys.exit('MONGODB_CONNECTION_STRING not set')
    mongo_database = os.environ.get('DB_NAME', None)
    if mongo_database is None:
        sys.exit('DB_NAME not set')
    if os.environ.get('NETWORK') not in {'mainnet', 'testnet'}:
        sys.exit("NETWORK should be 'mainnet' or 'testnet'")
    
    rpc_url = os.environ.get('RPC_URL', None)
    if rpc_url is None:
        sys.exit('RPC_URL not set')
    elif args.action == 'events':
        await run_events_transformer(mongo_url, mongo_database, rpc_url)
    elif args.action == 'positions':
        await run_positions_transformer(mongo_url, mongo_database, rpc_url)
    elif args.action == 'leaderboard':
        await run_leaderboard_transformer(mongo_url, mongo_database, rpc_url)
    elif args.action == 'graphql':
        await run_graphql_server(mongo_url, mongo_database)
    elif args.action == 'strk-calculation':
        await strk_rewards_calculation(mongo_url, mongo_database)
