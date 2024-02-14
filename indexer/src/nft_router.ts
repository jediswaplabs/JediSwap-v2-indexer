import {
    Block,
    EventWithTransaction,
  } from "../common/deps.ts";
import {
  COLLECTION_NAMES,
  SELECTOR_KEYS,
  NFT_ROUTER_CONTRACT,
  INDEX_FROM_BLOCK,
  MONGODB_CONNECTION_STRING,
  DB_NAME,
  STREAM_URL,
} from "../common/constants.ts";
import {
  fetchTokensFromPosition,
} from "../common/position_tokens.ts";
import {
  formatFelt, formatU256, senderAddress
} from "../common/utils.ts";
  
const filter = {
  header: { weak: true },
  events: [
    {
      fromAddress: NFT_ROUTER_CONTRACT,
      keys: [SELECTOR_KEYS.TRANSFER],
      includeTransaction: true,
      includeReceipt: false,
    }, {
      fromAddress: NFT_ROUTER_CONTRACT,
      keys: [SELECTOR_KEYS.INCREASE_LIQUIDITY],
      includeTransaction: true,
      includeReceipt: false,
    }, {
      fromAddress: NFT_ROUTER_CONTRACT,
      keys: [SELECTOR_KEYS.DECREASE_LIQUIDITY],
      includeTransaction: true,
      includeReceipt: false,
    }, {
      fromAddress: NFT_ROUTER_CONTRACT,
      keys: [SELECTOR_KEYS.COLLECT],
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
    collectionNames: [COLLECTION_NAMES.POSITIONS, COLLECTION_NAMES.POSITION_FEES],
    entityMode: true,
  },
};
    
export default async function transform({ header, events }: Block) {
  const output = (events ?? []).map(({ transaction, event }: EventWithTransaction) => {
    const txMeta = {
      positionAddress: formatFelt(event.fromAddress),
      timestamp: Date.parse(header?.timestamp),
      block: Number(header?.blockNumber),
      tx_hash: formatFelt(transaction.meta.hash),
      tx_sender: senderAddress(transaction),
    };
    const key = event.keys[0];
    switch (key) {
      case SELECTOR_KEYS.TRANSFER: {
        const positionId = Number(event.keys[3]);
        const ownerAddress = formatFelt(event.keys[2]);
        return {
          entity: { positionId },
          collection: COLLECTION_NAMES.POSITIONS,
          update: {
            "$set": {
              positionId,
              ownerAddress,
              ...txMeta,
            },
          },
        };
      }
      case SELECTOR_KEYS.INCREASE_LIQUIDITY: {
        const positionId = formatU256(event.data[0], event.data[1]);
        const liquidity = Number(event.data[2]);
        const amount0 = formatU256(event.data[3], event.data[4]);
        const amount1 = formatU256(event.data[5], event.data[6]);
        return {
          entity: { positionId },
          collection: COLLECTION_NAMES.POSITIONS,
          update: {
            "$set": {
              positionId,
              ...txMeta,
            },
            "$inc": {
              depositedToken0: amount0,
              depositedToken1: amount1,
              liquidity: liquidity,
            }
          },
        };
      };
      case SELECTOR_KEYS.DECREASE_LIQUIDITY: {
        const positionId = formatU256(event.data[0], event.data[1]);
        const liquidity = Number(event.data[2]);
        const amount0 = formatU256(event.data[3], event.data[4]);
        const amount1 = formatU256(event.data[5], event.data[6]);
        return {
          entity: { positionId },
          collection: COLLECTION_NAMES.POSITIONS,
          update: {
            "$set": {
              positionId,
              ...txMeta,
            },
            "$inc": {
              withdrawnToken0: amount0,
              withdrawnToken1: amount1,
              liquidity: -liquidity,
            }
          },
        };
      };
      case SELECTOR_KEYS.COLLECT: {
        const positionId = formatU256(event.data[0], event.data[1]);
        const ownerAddress = formatFelt(event.data[2]);
        const amount0_collect = Number(event.data[3]);
        const amount1_collect = Number(event.data[4]);
        return {
          entity: { positionId },
          collection: COLLECTION_NAMES.POSITION_FEES,
          update: {
            "$set": {
              positionId,
              ownerAddress,
              ...txMeta,
            },
            "$inc": {
              collectedFeesToken0: amount0_collect,
              collectedFeesToken1: amount1_collect,
            }
          },
        }
      };
      default:
        return;
    }
  }).filter(Boolean);;

  for (const inputEvent of output) {
    const positionId = inputEvent.entity.positionId;
    console.log(`Fetching tokens for position: ${positionId} ...`);
    const positionInfo = await fetchTokensFromPosition(NFT_ROUTER_CONTRACT, positionId);
    if (positionInfo) {
      inputEvent.update["$set"].token0Address = positionInfo.token0;
      inputEvent.update["$set"].token1Address = positionInfo.token1;
      console.log(`Tokens for position ${positionId} updated`);
    }
  }
  
  return output;
}
