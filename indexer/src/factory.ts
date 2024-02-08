import {
  Block,
  EventWithTransaction
} from "../common/deps.ts";
import {
  COLLECTION_NAMES,
  SELECTOR_KEYS,
  FACTORY_CONTRACT,
  INDEX_FROM_BLOCK,
  MONGODB_CONNECTION_STRING,
  DB_NAME,
  STREAM_URL,
  EVENTS,
} from "../common/constants.ts";
import {
  formatFelt, formatI256, formatU256, formatI32, senderAddress
} from "../common/utils.ts";

const filter = {
  header: { weak: true },
  events: [
    {
      fromAddress: FACTORY_CONTRACT,
      keys: [SELECTOR_KEYS.POOL_CREATED],
      includeTransaction: true,
      includeReceipt: false,
    },
  ],
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
    collectionNames: [COLLECTION_NAMES.POOLS, COLLECTION_NAMES.POOLS_DATA],
  },
};

export function factory({ header, events }: Block) {
  const poolEvents = (events ?? []).flatMap(({ event }: EventWithTransaction) => {
    const poolAddress = formatFelt(event.data[4]);
    return [
      SELECTOR_KEYS.INITIALIZE,
      SELECTOR_KEYS.MINT,
      SELECTOR_KEYS.SWAP,
      SELECTOR_KEYS.BURN,
      SELECTOR_KEYS.COLLECT,
    ].map((eventKey) => ({
      fromAddress: poolAddress,
      keys: [eventKey],
      includeTransaction: true,
      includeReceipt: false,
    }));
  });

  const pools_data = (events ?? []).flatMap(({ transaction, event }: EventWithTransaction) => {
    const data = {
      token0: formatFelt(event.data[0]),
      token1: formatFelt(event.data[1]),
      fee: Number(event.data[2]),
      tickSpacing: Number(event.data[3]),
      poolAddress: formatFelt(event.data[4]),
      timestamp: Date.parse(header?.timestamp),
      block: Number(header?.blockNumber),
      tx_hash: formatFelt(transaction.meta.hash),
      tx_sender: senderAddress(transaction)
    };
    return  {
      data,
      collection: COLLECTION_NAMES.POOLS,
    }
  });

  return {
    filter: {
      header: { weak: true },
      events: poolEvents,
    },
    data: pools_data
  };
}
  
export default function transform({ header, events }: Block) {
  const output = (events ?? []).map(({transaction, event }: EventWithTransaction) => {
    const txMeta = {
      poolAddress: formatFelt(event.fromAddress),
      timestamp: Date.parse(header?.timestamp),
      block: Number(header?.blockNumber),
      tx_hash: formatFelt(transaction.meta.hash),
      tx_sender: senderAddress(transaction)
    };
    const key = event.keys[0];
    switch (key) {
      case SELECTOR_KEYS.INITIALIZE: {
        const data = {
          event: EVENTS.INITIALIZE,
          sqrtPriceX96: formatU256(event.data[0], event.data[1]),
          tick: formatI32(event.data[2], event.data[3]),
          ...txMeta,
        }
        return {
          data,
          collection: COLLECTION_NAMES.POOLS_DATA,
        }
      };
      case SELECTOR_KEYS.MINT: {
        const data = {
          event: EVENTS.MINT,
          sender: formatFelt(event.data[0]),
          owner: formatFelt(event.data[1]),
          tickLower: formatI32(event.data[2], event.data[3]),
          tickUpper: formatI32(event.data[4], event.data[4]),
          amount: Number(event.data[6]),
          amount0: formatU256(event.data[7], event.data[8]),
          amount1: formatU256(event.data[9], event.data[10]),
          ...txMeta,
        }
        return {
          data,
          collection: COLLECTION_NAMES.POOLS_DATA,
        }
      };
      case SELECTOR_KEYS.BURN: {
        const data = {
          event: EVENTS.BURN,
          owner: formatFelt(event.data[0]),
          tickLower: formatI32(event.data[1], event.data[2]),
          tickUpper: formatI32(event.data[3], event.data[4]),
          amount: Number(event.data[5]),
          amount0: formatU256(event.data[6], event.data[7]),
          amount1: formatU256(event.data[8], event.data[9]),
          ...txMeta,
        }
        return {
          data,
          collection: COLLECTION_NAMES.POOLS_DATA,
        }
      };
      case SELECTOR_KEYS.SWAP: {
        const data = {
          event: EVENTS.SWAP,
          sender: formatFelt(event.data[0]),
          recipient: formatFelt(event.data[1]),
          amount0: formatI256(event.data[2], event.data[3], event.data[4]),
          amount1: formatI256(event.data[5], event.data[6], event.data[7]),
          sqrtPriceX96: formatU256(event.data[8], event.data[9]),
          liquidity: Number(event.data[10]),
          tick: formatI32(event.data[11], event.data[12]),
          ...txMeta,
        }
        return {
          data,
          collection: COLLECTION_NAMES.POOLS_DATA,
        }
      };
      case SELECTOR_KEYS.COLLECT: {
        const data = {
          event: EVENTS.COLLECT,
          owner: formatFelt(event.data[0]),
          recipient: formatFelt(event.data[1]),
          tickLower: formatI32(event.data[2], event.data[3]),
          tickUpper: formatI32(event.data[4], event.data[5]),
          amount0: Number(event.data[6]),
          amount1: Number(event.data[7]),
          ...txMeta,
        }
        return {
          data,
          collection: COLLECTION_NAMES.POOLS_DATA,
        }
      };
      default:
        return;
    }
  }).filter(Boolean);;

  return output;
}