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
  
  const filter = {
    header: { weak: true },
    events: [
      {
        fromAddress: NFT_ROUTER_CONTRACT,
        keys: [SELECTOR_KEYS.TRANSFER],
        includeTransaction: false,
        includeReceipt: false,
      }, {
        fromAddress: NFT_ROUTER_CONTRACT,
        keys: [SELECTOR_KEYS.INCREASE_LIQUIDITY],
        includeTransaction: false,
        includeReceipt: false,
      }, {
        fromAddress: NFT_ROUTER_CONTRACT,
        keys: [SELECTOR_KEYS.DECREASE_LIQUIDITY],
        includeTransaction: false,
        includeReceipt: false,
      }, {
        fromAddress: NFT_ROUTER_CONTRACT,
        keys: [SELECTOR_KEYS.COLLECT],
        includeTransaction: false,
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
    
  export default function transform({ header, events }: Block) {
    const output = events.map(({ event }: EventWithTransaction) => {
      const key = event.keys[0];
      switch (key) {
        case SELECTOR_KEYS.TRANSFER: {
          const positionId = Number(event.keys[3])
          const ownerAddress = event.keys[2]
          return {
            entity: { positionId },
            collection: COLLECTION_NAMES.POSITIONS,
            update: {
              "$set": {
                positionId,
                positionAddress: event.fromAddress,
                ownerAddress,
                timestamp: Date.parse(header?.timestamp),
                block: Number(header?.blockNumber),
              },
            },
          };
        }
        case SELECTOR_KEYS.INCREASE_LIQUIDITY: {
          const positionId = Number(event.data[0])
          const liquidity = Number(event.data[2])
          const amount0 = Number(event.data[3])
          const amount1 = Number(event.data[5])
          return {
            entity: { positionId },
            collection: COLLECTION_NAMES.POSITIONS,
            update: {
              "$set": {
                positionId,
                positionAddress: event.fromAddress,
                timestamp: Date.parse(header?.timestamp),
                block: Number(header?.blockNumber),
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
          const positionId = Number(event.data[0])
          const liquidity = Number(event.data[2])
          const amount0 = Number(event.data[3])
          const amount1 = Number(event.data[5])
          return {
            entity: { positionId },
            collection: COLLECTION_NAMES.POSITIONS,
            update: {
              "$set": {
                positionId,
                positionAddress: event.fromAddress,
                timestamp: Date.parse(header?.timestamp),
                block: Number(header?.blockNumber),
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
          const positionId = Number(event.data[0])
          const ownerAddress = event.data[2]
          const amount0_collect = Number(event.data[3])
          const amount1_collect = Number(event.data[4])
          return {
            entity: { positionId },
            collection: COLLECTION_NAMES.POSITION_FEES,
            update: {
              "$set": {
                positionId,
                positionAddress: event.fromAddress,
                ownerAddress,
                timestamp: Date.parse(header?.timestamp),
                block: Number(header?.blockNumber),
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

    return output;
  }
  