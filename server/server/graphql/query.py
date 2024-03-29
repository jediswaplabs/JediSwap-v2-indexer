from typing import List

import strawberry

from server.graphql.resolvers.pools import Pool, get_pools
from server.graphql.resolvers.nft_positions import NftPosition, get_nft_positions
from server.graphql.resolvers.nft_position_fees import NftPositionFee, get_nft_position_fees
from server.graphql.resolvers.tokens import Token, get_tokens
from server.graphql.resolvers.factories import Factory, get_factories
from server.graphql.resolvers.tokens_hour_data import TokenHourData, get_tokens_hour_data
from server.graphql.resolvers.tokens_day_data import TokenDayData, get_tokens_day_data
from server.graphql.resolvers.tokens_data import TokenData, get_tokens_data
from server.graphql.resolvers.pools_hour_data import PoolHourData, get_pools_hour_data
from server.graphql.resolvers.pools_day_data import PoolDayData, get_pools_day_data
from server.graphql.resolvers.pools_data import PoolData, get_pools_data
from server.graphql.resolvers.factories_day_data import FactoryDayData, get_factories_day_data
from server.graphql.resolvers.transactions import Transaction, get_transactions


@strawberry.type
class Query:
    pools: List[Pool] = strawberry.field(resolver=get_pools)
    nft_positions: List[NftPosition] = strawberry.field(resolver=get_nft_positions)
    nft_position_fees: List[NftPositionFee] = strawberry.field(resolver=get_nft_position_fees)
    tokens: List[Token] = strawberry.field(resolver=get_tokens)
    factories: List[Factory] = strawberry.field(resolver=get_factories)
    tokens_hour_data: List[TokenHourData] = strawberry.field(resolver=get_tokens_hour_data)
    tokens_day_data: List[TokenDayData] = strawberry.field(resolver=get_tokens_day_data)
    tokens_data: List[TokenData] = strawberry.field(resolver=get_tokens_data)
    pools_hour_data: List[PoolHourData] = strawberry.field(resolver=get_pools_hour_data)
    pools_day_data: List[PoolDayData] = strawberry.field(resolver=get_pools_day_data)
    pools_data: List[PoolData] = strawberry.field(resolver=get_pools_data)
    factories_day_data: List[FactoryDayData] = strawberry.field(resolver=get_factories_day_data)
    transactions: List[Transaction] = strawberry.field(resolver=get_transactions)
