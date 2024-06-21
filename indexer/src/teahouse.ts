import {
  Block,
  EventWithTransaction,
} from "../common/deps.ts";
import {
  COLLECTION_NAMES,
  SELECTOR_KEYS,
  TEAHOUSE_VAULT_CONTRACTS,
  INDEX_FROM_BLOCK,
  MONGODB_CONNECTION_STRING,
  DB_NAME,
  STREAM_URL,
  EVENTS,
} from "../common/constants.ts";
import {
  formatFelt, formatU256, senderAddress, formatI32
} from "../common/utils.ts";


const filter = {
  header: { weak: true },
  events: TEAHOUSE_VAULT_CONTRACTS.flatMap((teahouse_vault_contract) => ([SELECTOR_KEYS.ADD_LIQUIDITY, SELECTOR_KEYS.REMOVE_LIQUIDITY, SELECTOR_KEYS.COLLECT].map((eventKey) => ({
    fromAddress: teahouse_vault_contract,
    keys: [eventKey],
    includeTransaction: true,
    includeReceipt: false,
  })))),
};

export const config = {
  streamUrl: STREAM_URL,
  network: "starknet",
  filter,
  finality: "DATA_STATUS_ACCEPTED",
  startingBlock: INDEX_FROM_BLOCK,
  sinkType: "mongo",
  sinkOptions: {
    database: DB_NAME,
    connectionString: MONGODB_CONNECTION_STRING,
    collectionName: COLLECTION_NAMES.TEAHOUSE_VAULT_DATA,
  },
};


export default async function transform({ header, events }: Block) {
  const output = (events ?? []).map(({ transaction, event }: EventWithTransaction) => {
    const txMeta = {
      timestamp: Date.parse(header?.timestamp),
      block: Number(header?.blockNumber),
      tx_hash: formatFelt(transaction.meta.hash),
      tx_sender: senderAddress(transaction),
    };
    const key = event.keys[0];
    switch (key) {
      case SELECTOR_KEYS.ADD_LIQUIDITY: {
        const poolAddress = formatFelt(event.keys[1]);
        const tickLower = formatI32(event.data[0], event.data[1])
        const tickUpper = formatI32(event.data[2], event.data[3])
        const liquidity = Number(event.data[4]);
        const amount0 = formatU256(event.data[5], event.data[6]);
        const amount1 = formatU256(event.data[7], event.data[8]);
        return {
          event: EVENTS.ADD_LIQUIDITY,
          poolAddress,
          depositedToken0: amount0,
          depositedToken1: amount1,
          liquidity: liquidity,
          tickLower,
          tickUpper,
          ...txMeta,
        };
      };
      case SELECTOR_KEYS.REMOVE_LIQUIDITY: {
        const poolAddress = formatFelt(event.keys[1]);
        const tickLower = formatI32(event.data[0], event.data[1])
        const tickUpper = formatI32(event.data[2], event.data[3])
        const liquidity = Number(event.data[4]);
        const amount0 = formatU256(event.data[5], event.data[6]);
        const amount1 = formatU256(event.data[7], event.data[8]);
        return {
          event: EVENTS.REMOVE_LIQUIDITY,
          poolAddress,
          withdrawnToken0: amount0,
          withdrawnToken1: amount1,
          liquidity: liquidity,
          tickLower,
          tickUpper,
          ...txMeta,
        };
      };
      case SELECTOR_KEYS.COLLECT: {
        const poolAddress = formatFelt(event.keys[1]);
        const tickLower = formatI32(event.data[0], event.data[1])
        const tickUpper = formatI32(event.data[2], event.data[3])
        const amount0_collect = Number(event.data[4]);
        const amount1_collect = Number(event.data[5]);
        return {
          event: EVENTS.COLLECT,
          poolAddress,
          collectedFeesToken0: amount0_collect,
          collectedFeesToken1: amount1_collect,
          tickLower,
          tickUpper,
          ...txMeta,
        };
      };
      default:
        return;
    }
  }).filter(Boolean);;
  
  return output;
}
