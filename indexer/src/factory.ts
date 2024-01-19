import {
  Block,
  EventWithTransaction,
} from "../common/deps.ts";
import {
  COLLECTION_NAMES,
  SELECTOR_KEYS,
  FACTORY_CONTRACT,
  INDEX_FROM_BLOCK,
  MONGODB_CONNECTION_STRING,
  DB_NAME,
  STREAM_URL,
} from "../common/constants.ts";

const filter = {
  header: { weak: true },
  events: [
    {
      fromAddress: FACTORY_CONTRACT,
      keys: [SELECTOR_KEYS.POOL_CREATED],
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
    collectionName: COLLECTION_NAMES.POOLS,
    entityMode: true,
  },
};
  
export default function transform({ header, events }: Block) {
  const output = events.map(({ event }: EventWithTransaction) => {
    const key = event.keys[0];
    switch (key) {
      case SELECTOR_KEYS.POOL_CREATED: {
        const poolAddress = event.data[4]
        return {
          entity: { poolAddress },
          update: {
            "$set": {
              token0: event.data[0],
              token1: event.data[1],
              fee: Number(event.data[2]),
              tickSpacing: Number(event.data[3]),
              poolAddress: poolAddress,
              timestamp: Date.parse(header?.timestamp),
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
