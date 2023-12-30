from typing import List

import strawberry

from server.resolvers.pools import PoolCreated, get_pools
from server.resolvers.nft_positions import NftPosition, get_nft_positions
from server.resolvers.nft_position_fees import NftPositionFee, get_nft_position_fees
from server.resolvers.token import Token, get_tokens


@strawberry.type
class Query:
    pools: List[PoolCreated] = strawberry.field(resolver=get_pools)
    nft_positions: List[NftPosition] = strawberry.field(resolver=get_nft_positions)
    nft_position_fees: List[NftPositionFee] = strawberry.field(resolver=get_nft_position_fees)
    tokens: List[Token] = strawberry.field(resolver=get_tokens)
