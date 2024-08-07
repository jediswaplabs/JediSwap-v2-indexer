from typing import List

import strawberry

from server.graphql.resolvers.pools import Pool, get_pools
from server.graphql.resolvers.nft_positions import NftPosition, get_nft_positions
from server.graphql.resolvers.tokens import Token, get_tokens
from server.graphql.resolvers.factories import Factory, get_factories
from server.graphql.resolvers.tokens_hour_data import TokenHourData, get_tokens_hour_data
from server.graphql.resolvers.tokens_day_data import TokenDayData, get_tokens_day_data
from server.graphql.resolvers.tokens_data import TokenData, get_tokens_data
from server.graphql.resolvers.pools_hour_data import PoolHourData, get_pools_hour_data
from server.graphql.resolvers.pools_day_data import PoolDayData, get_pools_day_data
from server.graphql.resolvers.pools_data import PoolData, get_pools_data
from server.graphql.resolvers.factories_day_data import FactoryDayData, get_factories_day_data
from server.graphql.resolvers.factories_data import FactoriesData, get_factories_data
from server.graphql.resolvers.transactions import Transaction, get_transactions
from server.graphql.resolvers.lp_leaderboard import LpLeaderboard, get_lp_leaderboard_points
from server.graphql.resolvers.lp_leaderboard_snapshot import LpLeaderboardSnapshot, get_lp_leaderboard_snapshot
from server.graphql.resolvers.volume_leaderboard import VolumeLeaderboard, get_volume_leaderboard_points
from server.graphql.resolvers.volume_leaderboard_snapshot import VolumeLeaderboardSnapshot, get_volume_leaderboard_snapshot
from server.graphql.resolvers.strk_grant import fetch_strk_grant_data_v1, fetch_strk_grant_data_v2


@strawberry.type
class Query:
    pools: List[Pool] = strawberry.field(resolver=get_pools)
    nft_positions: List[NftPosition] = strawberry.field(resolver=get_nft_positions)
    tokens: List[Token] = strawberry.field(resolver=get_tokens)
    factories: List[Factory] = strawberry.field(resolver=get_factories)
    tokens_hour_data: List[TokenHourData] = strawberry.field(resolver=get_tokens_hour_data)
    tokens_day_data: List[TokenDayData] = strawberry.field(resolver=get_tokens_day_data)
    tokens_data: List[TokenData] = strawberry.field(resolver=get_tokens_data)
    pools_hour_data: List[PoolHourData] = strawberry.field(resolver=get_pools_hour_data)
    pools_day_data: List[PoolDayData] = strawberry.field(resolver=get_pools_day_data)
    pools_data: List[PoolData] = strawberry.field(resolver=get_pools_data)
    factories_day_data: List[FactoryDayData] = strawberry.field(resolver=get_factories_day_data)
    factories_data: List[FactoriesData] = strawberry.field(resolver=get_factories_data)
    transactions: List[Transaction] = strawberry.field(resolver=get_transactions)
    lp_leaderboard: List[LpLeaderboard] = strawberry.field(resolver=get_lp_leaderboard_points)
    lp_leaderboard_snapshot: List[LpLeaderboardSnapshot] = strawberry.field(resolver=get_lp_leaderboard_snapshot)
    volume_leaderboard: List[VolumeLeaderboard] = strawberry.field(resolver=get_volume_leaderboard_points)
    volume_leaderboard_snapshot: List[VolumeLeaderboardSnapshot] = strawberry.field(resolver=get_volume_leaderboard_snapshot)
    strk_grant_data: strawberry.scalars.JSON = strawberry.field(resolver=fetch_strk_grant_data_v1)
    strk_grant_data_v1: strawberry.scalars.JSON = strawberry.field(resolver=fetch_strk_grant_data_v1)
    strk_grant_data_v2: strawberry.scalars.JSON = strawberry.field(resolver=fetch_strk_grant_data_v2)
