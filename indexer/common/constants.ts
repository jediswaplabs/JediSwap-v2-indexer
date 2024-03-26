import { hash } from "./deps.ts";

export function formatSelectorKey(name: string): string {
  const key = BigInt(hash.getSelectorFromName(name));
  return `0x${key.toString(16).padStart(64, "0")}`;
}

export const COLLECTION_NAMES = {
  POOLS: 'pools',
  POOLS_DATA: 'pools_data',
  POSITIONS_DATA: 'positions_data',
}

export const EVENTS = {
  FEE_AMOUNT_ENABLED: 'FeeAmountEnabled',
  POOL_CREATED: 'PoolCreated',
  TRANSFER: 'Transfer',
  INCREASE_LIQUIDITY: 'IncreaseLiquidity',
  DECREASE_LIQUIDITY: 'DecreaseLiquidity',
  COLLECT: 'Collect',
  INITIALIZE: 'Initialize',
  MINT: 'Mint',
  BURN: 'Burn',
  SWAP: 'Swap',
}

export const SELECTOR_KEYS = {
  FEE_AMOUNT_ENABLED: formatSelectorKey(EVENTS.FEE_AMOUNT_ENABLED),
  POOL_CREATED: formatSelectorKey(EVENTS.POOL_CREATED),
  TRANSFER: formatSelectorKey(EVENTS.TRANSFER),
  INCREASE_LIQUIDITY: formatSelectorKey(EVENTS.INCREASE_LIQUIDITY),
  DECREASE_LIQUIDITY: formatSelectorKey(EVENTS.DECREASE_LIQUIDITY),
  COLLECT: formatSelectorKey(EVENTS.COLLECT),
  INITIALIZE: formatSelectorKey(EVENTS.INITIALIZE),
  MINT: formatSelectorKey(EVENTS.MINT),
  BURN: formatSelectorKey(EVENTS.BURN),
  SWAP: formatSelectorKey(EVENTS.SWAP),
};

export const FACTORY_CONTRACT = Deno.env.get(
  "FACTORY_CONTRACT"
) as string;
const NFT_ROUTER_CONTRACTS_STRING = Deno.env.get(
  "NFT_ROUTER_CONTRACTS"
) as string;
export const NFT_ROUTER_CONTRACTS = NFT_ROUTER_CONTRACTS_STRING.split(",");
export const INDEX_FROM_BLOCK = Number(Deno.env.get(
  "INDEX_FROM_BLOCK"))
export const STREAM_URL = Deno.env.get(
  "STREAM_URL"
) as string;
export const MONGODB_CONNECTION_STRING = Deno.env.get(
  "MONGODB_CONNECTION_STRING"
) as string;
export const DB_NAME = Deno.env.get(
  "DB_NAME"
) as string;
