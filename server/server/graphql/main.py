import asyncio

import aiohttp_cors
import strawberry
from typing import List

from aiohttp import web
from pymongo import MongoClient
from pymongo.database import Database
from strawberry.aiohttp.views import GraphQLView
from strawberry.dataloader import DataLoader

from server.graphql.query import Query

from server.graphql.resolvers.pools import Pool, get_pool
from server.graphql.resolvers.tokens import Token, get_token

async def load_pools(db, keys) -> List[Pool]:
    return [get_pool(db, key) for key in keys]

async def load_tokens(db, keys) -> List[Token]:
    return [get_token(db, key) for key in keys]


class IndexerGraphQLView(GraphQLView):
    def __init__(self, db, **kwargs):
        super().__init__(**kwargs)
        self._db = db

    async def get_context(self, request, response):
        return {'db': self._db,
                "pool_loader": DataLoader(load_fn=lambda ids: load_pools(self._db, ids)),
                "token_loader": DataLoader(load_fn=lambda ids: load_tokens(self._db, ids))}


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