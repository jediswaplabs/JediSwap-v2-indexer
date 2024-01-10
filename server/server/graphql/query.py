from typing import List

import strawberry

from server.graphql.resolvers.pools import PoolCreated, get_pools
from server.graphql.resolvers.nft_positions import NftPosition, get_nft_positions
from server.graphql.resolvers.nft_position_fees import NftPositionFee, get_nft_position_fees
from server.graphql.resolvers.tokens import Token, get_tokens
from server.graphql.resolvers.factories import Factory, get_factories


@strawberry.type
class Query:
    pools: List[PoolCreated] = strawberry.field(resolver=get_pools)
    nft_positions: List[NftPosition] = strawberry.field(resolver=get_nft_positions)
    nft_position_fees: List[NftPositionFee] = strawberry.field(resolver=get_nft_position_fees)
    tokens: List[Token] = strawberry.field(resolver=get_tokens)
    factories: List[Factory] = strawberry.field(resolver=get_factories)
