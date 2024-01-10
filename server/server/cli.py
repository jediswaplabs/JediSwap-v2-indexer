import argparse
import os
import sys

from dotenv import load_dotenv

from server.transform.init_db import run as init_run
from server.transform.process_events import run as process_run
from server.graphql.main import run as graphql_run


load_dotenv()


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['init', 'process', 'graphql'], help='Choose action: init or process or graphql')

    args = parser.parse_args()

    mongo_url = os.environ.get('MONGO_URL', None)
    if mongo_url is None:
        sys.exit('MONGO_URL not set')
    mongo_database = os.environ.get('MONGO_DB', None)
    if mongo_database is None:
        sys.exit('MONGO_DB not set')
    if os.environ.get('NETWORK') not in {'mainnet', 'testnet'}:
        sys.exit("NETWORK should be 'mainnet' or 'testnet'")
    rpc_url = os.environ.get('RPC_URL', None)
    if rpc_url is None:
        sys.exit('RPC_URL not set')

    if args.action == 'init':
        init_run(mongo_url, mongo_database, rpc_url)
    elif args.action == 'process':
        process_run(mongo_url, mongo_database, rpc_url)
    elif args.action == 'graphql':
        graphql_run(mongo_url, mongo_database)