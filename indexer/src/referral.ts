import {
  Block,
  EventWithTransaction
} from "../common/deps.ts";
import {
  COLLECTION_NAMES,
  SELECTOR_KEYS,
  REFERRAL_CONTRACT,
  INDEX_FROM_BLOCK,
  MONGODB_CONNECTION_STRING,
  DB_NAME,
  STREAM_URL,
  EVENTS,
} from "../common/constants.ts";
import {
  formatFelt, senderAddress
} from "../common/utils.ts";

const filter = {
  header: { weak: true },
  events: [
    {
      fromAddress: REFERRAL_CONTRACT,
      keys: [SELECTOR_KEYS.SET_REFERRER],
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
    collectionName: COLLECTION_NAMES.REFERRAL_DATA,
  },
};
  
export default function transform({ header, events }: Block) {
  const output = (events ?? []).map(({transaction, event }: EventWithTransaction) => {
    const txMeta = {
      timestamp: Date.parse(header?.timestamp),
      block: Number(header?.blockNumber),
      tx_hash: formatFelt(transaction.meta.hash),
      tx_sender: senderAddress(transaction)
    };
    const key = event.keys[0];
    switch (key) {
      case SELECTOR_KEYS.SET_REFERRER: {
        return {
          account: formatFelt(event.data[0]),
          referrer: formatFelt(event.data[1]),
          ...txMeta,
        }
      };
      default:
        return;
    }
  }).filter(Boolean);;

  return output;
}