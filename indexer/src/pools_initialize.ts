import {
    Block,
    EventWithTransaction,
  } from "../common/deps.ts";
  import {
    COLLECTION_NAMES,
    SELECTOR_KEYS,
    POOL_CONTRACT,
    INDEX_FROM_BLOCK,
    MONGODB_CONNECTION_STRING,
    DB_NAME,
    STREAM_URL,
  } from "../common/constants.ts";
  
  const filter = {
    header: { weak: true },
    events: [
      {
        fromAddress: POOL_CONTRACT,
        keys: [SELECTOR_KEYS.INITIALIZE],
        includeTransaction: false,
        includeReceipt: false,
      }
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
      collectionName: COLLECTION_NAMES.POOLS,
      entityMode: true,
    },
  };
    
  export default function transform({ header, events }: Block) {
    const output = events.map(({ event }: EventWithTransaction) => {
      const key = event.keys[0];
      switch (key) {
        case SELECTOR_KEYS.INITIALIZE: {
          const poolAddress = event.fromAddress;
          const sqrtPriceX96 = Number(event.data[0]);
          const tick = event.data[2];
          return {
            entity: { poolAddress },
            update: {
              "$set": {
                sqrtPriceX96,
                tick,
                timestamp: header?.timestamp,
                block: Number(header?.blockNumber),
              },
            },
          };
        };
        default:
          return;
      }
    }).filter(Boolean);;

    return output;
  }
  