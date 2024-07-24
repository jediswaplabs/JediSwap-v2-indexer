import {
    Block,
  } from "../common/deps.ts";
import {
  COLLECTION_NAMES,
  INDEX_FROM_BLOCK,
  MONGODB_CONNECTION_STRING,
  DB_NAME,
  STREAM_URL,
} from "../common/constants.ts";
  
const filter = {
  header: { weak: false },
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
    collectionName: COLLECTION_NAMES.BLOCKS,
  },
};
    
export default async function transform({ header }: Block) {
  const { blockNumber, blockHash, timestamp } = header;
  const date = new Date(timestamp);
  const blockTimestamp = date.getTime();
  return [{
    blockNumber,
    blockHash,
    timestamp: blockTimestamp,
  }];
}
