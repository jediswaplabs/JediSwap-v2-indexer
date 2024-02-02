import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from server.graphql.main import run as graphql_run
from server.transform.process_events import run as process_run
from server.transform.process_positions import run as positions_run


ENV_FILE = Path(__file__).parent.parent.parent / 'env_goerli'


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['init', 'process', 'graphql', 'positions'], 
                        help='Choose action: init or process or graphql or positions')
    parser.add_argument('--env-file', help='Run with mainnet config')

    args = parser.parse_args()
    
    env_file = args.env_file if args.env_file else ENV_FILE
    load_dotenv(env_file)

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

    elif args.action == 'process':
        process_run(mongo_url, mongo_database, rpc_url)
    elif args.action == 'graphql':
        graphql_run(mongo_url, mongo_database)
    elif args.action == 'positions':
        positions_run(mongo_url, mongo_database, rpc_url)
