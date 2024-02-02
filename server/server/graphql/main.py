import asyncio

import aiohttp_cors
import strawberry

from aiohttp import web
from pymongo import MongoClient
from pymongo.database import Database
from strawberry.aiohttp.views import GraphQLView

from server.graphql.query import Query


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

    print(f'GraphQL server started on port 8000')

    while True:
        await asyncio.sleep(5_000)


def run(mongo_url: str, mongo_database: Database):
    print('Starting server')
    asyncio.run(run_graphql_server(mongo_url, mongo_database))