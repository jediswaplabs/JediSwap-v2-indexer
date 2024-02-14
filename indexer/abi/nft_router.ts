import { type Abi } from "../common/deps.ts";

export const nftPositionAbi = [
    {
        "name": "ERC721MetadataImpl",
        "type": "impl",
        "interface_name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::IERC721Metadata"
    },
    {
        "name": "core::integer::u256",
        "type": "struct",
        "members": [
        {
            "name": "low",
            "type": "core::integer::u128"
        },
        {
            "name": "high",
            "type": "core::integer::u128"
        }
        ]
    },
    {
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::IERC721Metadata",
        "type": "interface",
        "items": [
        {
            "name": "name",
            "type": "function",
            "inputs": [],
            "outputs": [
            {
                "type": "core::felt252"
            }
            ],
            "state_mutability": "view"
        },
        {
            "name": "symbol",
            "type": "function",
            "inputs": [],
            "outputs": [
            {
                "type": "core::felt252"
            }
            ],
            "state_mutability": "view"
        },
        {
            "name": "token_uri",
            "type": "function",
            "inputs": [
            {
                "name": "token_id",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [
            {
                "type": "core::array::Array::<core::felt252>"
            }
            ],
            "state_mutability": "view"
        }
        ]
    },
    {
        "name": "ERC721CamelMetadataImpl",
        "type": "impl",
        "interface_name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::IERC721CamelMetadata"
    },
    {
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::IERC721CamelMetadata",
        "type": "interface",
        "items": [
        {
            "name": "tokenURI",
            "type": "function",
            "inputs": [
            {
                "name": "token_id",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [
            {
                "type": "core::array::Array::<core::felt252>"
            }
            ],
            "state_mutability": "view"
        }
        ]
    },
    {
        "name": "JediSwapV2NFTPositionManagerImpl",
        "type": "impl",
        "interface_name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::IJediSwapV2NFTPositionManager"
    },
    {
        "name": "core::bool",
        "type": "enum",
        "variants": [
        {
            "name": "False",
            "type": "()"
        },
        {
            "name": "True",
            "type": "()"
        }
        ]
    },
    {
        "name": "yas_core::numbers::signed_integer::i32::i32",
        "type": "struct",
        "members": [
        {
            "name": "mag",
            "type": "core::integer::u32"
        },
        {
            "name": "sign",
            "type": "core::bool"
        }
        ]
    },
    {
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::PositionDetail",
        "type": "struct",
        "members": [
        {
            "name": "operator",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "name": "pool_id",
            "type": "core::integer::u64"
        },
        {
            "name": "tick_lower",
            "type": "yas_core::numbers::signed_integer::i32::i32"
        },
        {
            "name": "tick_upper",
            "type": "yas_core::numbers::signed_integer::i32::i32"
        },
        {
            "name": "liquidity",
            "type": "core::integer::u128"
        },
        {
            "name": "fee_growth_inside_0_last_X128",
            "type": "core::integer::u256"
        },
        {
            "name": "fee_growth_inside_1_last_X128",
            "type": "core::integer::u256"
        },
        {
            "name": "tokens_owed_0",
            "type": "core::integer::u128"
        },
        {
            "name": "tokens_owed_1",
            "type": "core::integer::u128"
        }
        ]
    },
    {
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::PoolKey",
        "type": "struct",
        "members": [
        {
            "name": "token0",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "name": "token1",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "name": "fee",
            "type": "core::integer::u32"
        }
        ]
    },
    {
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::MintParams",
        "type": "struct",
        "members": [
        {
            "name": "token0",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "name": "token1",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "name": "fee",
            "type": "core::integer::u32"
        },
        {
            "name": "tick_lower",
            "type": "yas_core::numbers::signed_integer::i32::i32"
        },
        {
            "name": "tick_upper",
            "type": "yas_core::numbers::signed_integer::i32::i32"
        },
        {
            "name": "amount0_desired",
            "type": "core::integer::u256"
        },
        {
            "name": "amount1_desired",
            "type": "core::integer::u256"
        },
        {
            "name": "amount0_min",
            "type": "core::integer::u256"
        },
        {
            "name": "amount1_min",
            "type": "core::integer::u256"
        },
        {
            "name": "recipient",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "name": "deadline",
            "type": "core::integer::u64"
        }
        ]
    },
    {
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::IncreaseLiquidityParams",
        "type": "struct",
        "members": [
        {
            "name": "token_id",
            "type": "core::integer::u256"
        },
        {
            "name": "amount0_desired",
            "type": "core::integer::u256"
        },
        {
            "name": "amount1_desired",
            "type": "core::integer::u256"
        },
        {
            "name": "amount0_min",
            "type": "core::integer::u256"
        },
        {
            "name": "amount1_min",
            "type": "core::integer::u256"
        },
        {
            "name": "deadline",
            "type": "core::integer::u64"
        }
        ]
    },
    {
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::DecreaseLiquidityParams",
        "type": "struct",
        "members": [
        {
            "name": "token_id",
            "type": "core::integer::u256"
        },
        {
            "name": "liquidity",
            "type": "core::integer::u128"
        },
        {
            "name": "amount0_min",
            "type": "core::integer::u256"
        },
        {
            "name": "amount1_min",
            "type": "core::integer::u256"
        },
        {
            "name": "deadline",
            "type": "core::integer::u64"
        }
        ]
    },
    {
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::CollectParams",
        "type": "struct",
        "members": [
        {
            "name": "token_id",
            "type": "core::integer::u256"
        },
        {
            "name": "recipient",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "name": "amount0_max",
            "type": "core::integer::u128"
        },
        {
            "name": "amount1_max",
            "type": "core::integer::u128"
        }
        ]
    },
    {
        "name": "core::array::Span::<core::felt252>",
        "type": "struct",
        "members": [
        {
            "name": "snapshot",
            "type": "@core::array::Array::<core::felt252>"
        }
        ]
    },
    {
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::IJediSwapV2NFTPositionManager",
        "type": "interface",
        "items": [
        {
            "name": "get_factory",
            "type": "function",
            "inputs": [],
            "outputs": [
            {
                "type": "core::starknet::contract_address::ContractAddress"
            }
            ],
            "state_mutability": "view"
        },
        {
            "name": "get_position",
            "type": "function",
            "inputs": [
            {
                "name": "token_id",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [
            {
                "type": "(jediswap_v2_periphery::jediswap_v2_nft_position_manager::PositionDetail, jediswap_v2_periphery::jediswap_v2_nft_position_manager::PoolKey)"
            }
            ],
            "state_mutability": "view"
        },
        {
            "name": "mint",
            "type": "function",
            "inputs": [
            {
                "name": "params",
                "type": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::MintParams"
            }
            ],
            "outputs": [
            {
                "type": "(core::integer::u256, core::integer::u128, core::integer::u256, core::integer::u256)"
            }
            ],
            "state_mutability": "external"
        },
        {
            "name": "increase_liquidity",
            "type": "function",
            "inputs": [
            {
                "name": "params",
                "type": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::IncreaseLiquidityParams"
            }
            ],
            "outputs": [
            {
                "type": "(core::integer::u128, core::integer::u256, core::integer::u256)"
            }
            ],
            "state_mutability": "external"
        },
        {
            "name": "decrease_liquidity",
            "type": "function",
            "inputs": [
            {
                "name": "params",
                "type": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::DecreaseLiquidityParams"
            }
            ],
            "outputs": [
            {
                "type": "(core::integer::u256, core::integer::u256)"
            }
            ],
            "state_mutability": "external"
        },
        {
            "name": "collect",
            "type": "function",
            "inputs": [
            {
                "name": "params",
                "type": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::CollectParams"
            }
            ],
            "outputs": [
            {
                "type": "(core::integer::u128, core::integer::u128)"
            }
            ],
            "state_mutability": "external"
        },
        {
            "name": "burn",
            "type": "function",
            "inputs": [
            {
                "name": "token_id",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [],
            "state_mutability": "external"
        },
        {
            "name": "create_and_initialize_pool",
            "type": "function",
            "inputs": [
            {
                "name": "token0",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "token1",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "fee",
                "type": "core::integer::u32"
            },
            {
                "name": "sqrt_price_X96",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [
            {
                "type": "core::starknet::contract_address::ContractAddress"
            }
            ],
            "state_mutability": "external"
        },
        {
            "name": "jediswap_v2_mint_callback",
            "type": "function",
            "inputs": [
            {
                "name": "amount0_owed",
                "type": "core::integer::u256"
            },
            {
                "name": "amount1_owed",
                "type": "core::integer::u256"
            },
            {
                "name": "callback_data_span",
                "type": "core::array::Span::<core::felt252>"
            }
            ],
            "outputs": [],
            "state_mutability": "external"
        }
        ]
    },
    {
        "name": "ERC721Impl",
        "type": "impl",
        "interface_name": "openzeppelin::token::erc721::interface::IERC721"
    },
    {
        "name": "openzeppelin::token::erc721::interface::IERC721",
        "type": "interface",
        "items": [
        {
            "name": "balance_of",
            "type": "function",
            "inputs": [
            {
                "name": "account",
                "type": "core::starknet::contract_address::ContractAddress"
            }
            ],
            "outputs": [
            {
                "type": "core::integer::u256"
            }
            ],
            "state_mutability": "view"
        },
        {
            "name": "owner_of",
            "type": "function",
            "inputs": [
            {
                "name": "token_id",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [
            {
                "type": "core::starknet::contract_address::ContractAddress"
            }
            ],
            "state_mutability": "view"
        },
        {
            "name": "safe_transfer_from",
            "type": "function",
            "inputs": [
            {
                "name": "from",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "to",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "token_id",
                "type": "core::integer::u256"
            },
            {
                "name": "data",
                "type": "core::array::Span::<core::felt252>"
            }
            ],
            "outputs": [],
            "state_mutability": "external"
        },
        {
            "name": "transfer_from",
            "type": "function",
            "inputs": [
            {
                "name": "from",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "to",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "token_id",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [],
            "state_mutability": "external"
        },
        {
            "name": "approve",
            "type": "function",
            "inputs": [
            {
                "name": "to",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "token_id",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [],
            "state_mutability": "external"
        },
        {
            "name": "set_approval_for_all",
            "type": "function",
            "inputs": [
            {
                "name": "operator",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "approved",
                "type": "core::bool"
            }
            ],
            "outputs": [],
            "state_mutability": "external"
        },
        {
            "name": "get_approved",
            "type": "function",
            "inputs": [
            {
                "name": "token_id",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [
            {
                "type": "core::starknet::contract_address::ContractAddress"
            }
            ],
            "state_mutability": "view"
        },
        {
            "name": "is_approved_for_all",
            "type": "function",
            "inputs": [
            {
                "name": "owner",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "operator",
                "type": "core::starknet::contract_address::ContractAddress"
            }
            ],
            "outputs": [
            {
                "type": "core::bool"
            }
            ],
            "state_mutability": "view"
        }
        ]
    },
    {
        "name": "ERC721CamelOnlyImpl",
        "type": "impl",
        "interface_name": "openzeppelin::token::erc721::interface::IERC721CamelOnly"
    },
    {
        "name": "openzeppelin::token::erc721::interface::IERC721CamelOnly",
        "type": "interface",
        "items": [
        {
            "name": "balanceOf",
            "type": "function",
            "inputs": [
            {
                "name": "account",
                "type": "core::starknet::contract_address::ContractAddress"
            }
            ],
            "outputs": [
            {
                "type": "core::integer::u256"
            }
            ],
            "state_mutability": "view"
        },
        {
            "name": "ownerOf",
            "type": "function",
            "inputs": [
            {
                "name": "tokenId",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [
            {
                "type": "core::starknet::contract_address::ContractAddress"
            }
            ],
            "state_mutability": "view"
        },
        {
            "name": "safeTransferFrom",
            "type": "function",
            "inputs": [
            {
                "name": "from",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "to",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "tokenId",
                "type": "core::integer::u256"
            },
            {
                "name": "data",
                "type": "core::array::Span::<core::felt252>"
            }
            ],
            "outputs": [],
            "state_mutability": "external"
        },
        {
            "name": "transferFrom",
            "type": "function",
            "inputs": [
            {
                "name": "from",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "to",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "tokenId",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [],
            "state_mutability": "external"
        },
        {
            "name": "setApprovalForAll",
            "type": "function",
            "inputs": [
            {
                "name": "operator",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "approved",
                "type": "core::bool"
            }
            ],
            "outputs": [],
            "state_mutability": "external"
        },
        {
            "name": "getApproved",
            "type": "function",
            "inputs": [
            {
                "name": "tokenId",
                "type": "core::integer::u256"
            }
            ],
            "outputs": [
            {
                "type": "core::starknet::contract_address::ContractAddress"
            }
            ],
            "state_mutability": "view"
        },
        {
            "name": "isApprovedForAll",
            "type": "function",
            "inputs": [
            {
                "name": "owner",
                "type": "core::starknet::contract_address::ContractAddress"
            },
            {
                "name": "operator",
                "type": "core::starknet::contract_address::ContractAddress"
            }
            ],
            "outputs": [
            {
                "type": "core::bool"
            }
            ],
            "state_mutability": "view"
        }
        ]
    },
    {
        "name": "SRC5Impl",
        "type": "impl",
        "interface_name": "openzeppelin::introspection::interface::ISRC5"
    },
    {
        "name": "openzeppelin::introspection::interface::ISRC5",
        "type": "interface",
        "items": [
        {
            "name": "supports_interface",
            "type": "function",
            "inputs": [
            {
                "name": "interface_id",
                "type": "core::felt252"
            }
            ],
            "outputs": [
            {
                "type": "core::bool"
            }
            ],
            "state_mutability": "view"
        }
        ]
    },
    {
        "name": "SRC5CamelImpl",
        "type": "impl",
        "interface_name": "openzeppelin::introspection::interface::ISRC5Camel"
    },
    {
        "name": "openzeppelin::introspection::interface::ISRC5Camel",
        "type": "interface",
        "items": [
        {
            "name": "supportsInterface",
            "type": "function",
            "inputs": [
            {
                "name": "interfaceId",
                "type": "core::felt252"
            }
            ],
            "outputs": [
            {
                "type": "core::bool"
            }
            ],
            "state_mutability": "view"
        }
        ]
    },
    {
        "name": "constructor",
        "type": "constructor",
        "inputs": [
        {
            "name": "factory",
            "type": "core::starknet::contract_address::ContractAddress"
        }
        ]
    },
    {
        "kind": "struct",
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::JediSwapV2NFTPositionManager::IncreaseLiquidity",
        "type": "event",
        "members": [
        {
            "kind": "data",
            "name": "token_id",
            "type": "core::integer::u256"
        },
        {
            "kind": "data",
            "name": "liquidity",
            "type": "core::integer::u128"
        },
        {
            "kind": "data",
            "name": "amount0",
            "type": "core::integer::u256"
        },
        {
            "kind": "data",
            "name": "amount1",
            "type": "core::integer::u256"
        }
        ]
    },
    {
        "kind": "struct",
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::JediSwapV2NFTPositionManager::DecreaseLiquidity",
        "type": "event",
        "members": [
        {
            "kind": "data",
            "name": "token_id",
            "type": "core::integer::u256"
        },
        {
            "kind": "data",
            "name": "liquidity",
            "type": "core::integer::u128"
        },
        {
            "kind": "data",
            "name": "amount0",
            "type": "core::integer::u256"
        },
        {
            "kind": "data",
            "name": "amount1",
            "type": "core::integer::u256"
        }
        ]
    },
    {
        "kind": "struct",
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::JediSwapV2NFTPositionManager::Collect",
        "type": "event",
        "members": [
        {
            "kind": "data",
            "name": "token_id",
            "type": "core::integer::u256"
        },
        {
            "kind": "data",
            "name": "recipient",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "kind": "data",
            "name": "amount0_collect",
            "type": "core::integer::u128"
        },
        {
            "kind": "data",
            "name": "amount1_collect",
            "type": "core::integer::u128"
        }
        ]
    },
    {
        "kind": "struct",
        "name": "openzeppelin::token::erc721::erc721::ERC721Component::Transfer",
        "type": "event",
        "members": [
        {
            "kind": "key",
            "name": "from",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "kind": "key",
            "name": "to",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "kind": "key",
            "name": "token_id",
            "type": "core::integer::u256"
        }
        ]
    },
    {
        "kind": "struct",
        "name": "openzeppelin::token::erc721::erc721::ERC721Component::Approval",
        "type": "event",
        "members": [
        {
            "kind": "key",
            "name": "owner",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "kind": "key",
            "name": "approved",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "kind": "key",
            "name": "token_id",
            "type": "core::integer::u256"
        }
        ]
    },
    {
        "kind": "struct",
        "name": "openzeppelin::token::erc721::erc721::ERC721Component::ApprovalForAll",
        "type": "event",
        "members": [
        {
            "kind": "key",
            "name": "owner",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "kind": "key",
            "name": "operator",
            "type": "core::starknet::contract_address::ContractAddress"
        },
        {
            "kind": "data",
            "name": "approved",
            "type": "core::bool"
        }
        ]
    },
    {
        "kind": "enum",
        "name": "openzeppelin::token::erc721::erc721::ERC721Component::Event",
        "type": "event",
        "variants": [
        {
            "kind": "nested",
            "name": "Transfer",
            "type": "openzeppelin::token::erc721::erc721::ERC721Component::Transfer"
        },
        {
            "kind": "nested",
            "name": "Approval",
            "type": "openzeppelin::token::erc721::erc721::ERC721Component::Approval"
        },
        {
            "kind": "nested",
            "name": "ApprovalForAll",
            "type": "openzeppelin::token::erc721::erc721::ERC721Component::ApprovalForAll"
        }
        ]
    },
    {
        "kind": "enum",
        "name": "openzeppelin::introspection::src5::SRC5Component::Event",
        "type": "event",
        "variants": []
    },
    {
        "kind": "enum",
        "name": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::JediSwapV2NFTPositionManager::Event",
        "type": "event",
        "variants": [
        {
            "kind": "nested",
            "name": "IncreaseLiquidity",
            "type": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::JediSwapV2NFTPositionManager::IncreaseLiquidity"
        },
        {
            "kind": "nested",
            "name": "DecreaseLiquidity",
            "type": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::JediSwapV2NFTPositionManager::DecreaseLiquidity"
        },
        {
            "kind": "nested",
            "name": "Collect",
            "type": "jediswap_v2_periphery::jediswap_v2_nft_position_manager::JediSwapV2NFTPositionManager::Collect"
        },
        {
            "kind": "flat",
            "name": "ERC721Event",
            "type": "openzeppelin::token::erc721::erc721::ERC721Component::Event"
        },
        {
            "kind": "flat",
            "name": "SRC5Event",
            "type": "openzeppelin::introspection::src5::SRC5Component::Event"
        }
        ]
    }
] satisfies Abi;