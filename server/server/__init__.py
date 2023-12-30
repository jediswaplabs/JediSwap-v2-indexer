import asyncio
import logging
import os
import sys

import aiohttp_cors
import strawberry
from aiohttp import web
from pymongo import MongoClient
from strawberry.aiohttp.views import GraphQLView

from server.query import Query


class IndexerGraphQLView(GraphQLView):
    def __init__(self, db, **kwargs):
        super().__init__(**kwargs)
        self._db = db

    async def get_context(self, request, response):
        return {'db': self._db}


async def run_graphql_server(mongo_url, mongo_database):
    mongo = MongoClient(mongo_url)
    db_name = mongo_database.replace('-', '_')
    db = mongo[db_name]

    schema = strawberry.Schema(query=Query)
    view = IndexerGraphQLView(db, schema=schema)

    app = web.Application()
    cors = aiohttp_cors.setup(
        app,
        defaults={
            '*': aiohttp_cors.ResourceOptions(
                allow_credentials=True, allow_headers='*', allow_methods='*'
            )
        },
    )

    resource = cors.add(app.router.add_resource('/graphql'))
    cors.add(resource.add_route('*', view))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', '8000')
    await site.start()

    logging.info(f'GraphQL server started on port 8000')

    while True:
        await asyncio.sleep(5_000)


def run():
    logging.info('Starting server')
    mongo_url = os.environ.get('MONGO_URL', None)
    if mongo_url is None:
        sys.exit('MONGO_URL not set')
    mongo_database = os.environ.get('MONGO_DB', None)
    if mongo_database is None:
        sys.exit('MONGO_DB not set')

    asyncio.run(run_graphql_server(mongo_url, mongo_database))