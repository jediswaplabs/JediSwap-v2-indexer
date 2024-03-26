import {
    Block,
    EventWithTransaction,
  } from "../common/deps.ts";
import {
  COLLECTION_NAMES,
  SELECTOR_KEYS,
  NFT_ROUTER_CONTRACTS,
  INDEX_FROM_BLOCK,
  MONGODB_CONNECTION_STRING,
  DB_NAME,
  STREAM_URL,
  EVENTS,
} from "../common/constants.ts";
import {
  fetchAdditionalDetailsFromPosition,
} from "../common/position_details.ts";
import {
  formatFelt, formatU256, senderAddress
} from "../common/utils.ts";
  
const filter = {
  header: { weak: true },
  events: NFT_ROUTER_CONTRACTS.flatMap((nft_router_contract) => ([SELECTOR_KEYS.TRANSFER, SELECTOR_KEYS.INCREASE_LIQUIDITY, SELECTOR_KEYS.DECREASE_LIQUIDITY, SELECTOR_KEYS.COLLECT].map((eventKey) => ({
    fromAddress: nft_router_contract,
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
    collectionName: COLLECTION_NAMES.POSITIONS_DATA,
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
      case SELECTOR_KEYS.TRANSFER: {
        const positionId = Number(event.keys[3]);
        const positionAddress = formatFelt(event.fromAddress);
        const ownerAddress = formatFelt(event.keys[2]);
        return {
          event: EVENTS.TRANSFER,
          positionId,
          positionAddress,
          ownerAddress,
          ...txMeta,
        };
      }
      case SELECTOR_KEYS.INCREASE_LIQUIDITY: {
        const positionId = formatU256(event.data[0], event.data[1]);
        const positionAddress = formatFelt(event.fromAddress);
        const liquidity = Number(event.data[2]);
        const amount0 = formatU256(event.data[3], event.data[4]);
        const amount1 = formatU256(event.data[5], event.data[6]);
        return {
          event: EVENTS.INCREASE_LIQUIDITY,
          positionId,
          positionAddress,
          depositedToken0: amount0,
          depositedToken1: amount1,
          liquidity: liquidity,
          ...txMeta,
        };
      };
      case SELECTOR_KEYS.DECREASE_LIQUIDITY: {
        const positionId = formatU256(event.data[0], event.data[1]);
        const positionAddress = formatFelt(event.fromAddress);
        const liquidity = Number(event.data[2]);
        const amount0 = formatU256(event.data[3], event.data[4]);
        const amount1 = formatU256(event.data[5], event.data[6]);
        return {
          event: EVENTS.DECREASE_LIQUIDITY,
          positionId,
          positionAddress,
          withdrawnToken0: amount0,
          withdrawnToken1: amount1,
          liquidity: liquidity,
          ...txMeta,
        };
      };
      case SELECTOR_KEYS.COLLECT: {
        const positionId = formatU256(event.data[0], event.data[1]);
        const positionAddress = formatFelt(event.fromAddress);
        const ownerAddress = formatFelt(event.data[2]);
        const amount0_collect = Number(event.data[3]);
        const amount1_collect = Number(event.data[4]);
        return {
          event: EVENTS.COLLECT,
          positionId,
          positionAddress,
          ownerAddress,
          collectedFeesToken0: amount0_collect,
          collectedFeesToken1: amount1_collect,
          ...txMeta,
        };
      };
      default:
        return;
    }
  }).filter(Boolean);;

  for (const inputEvent of output) {
    const positionId = inputEvent.positionId
    const positionAddress = inputEvent.positionAddress;
    console.log(`Fetching additonal details for position: ${positionId} ...`);
    const positionInfo = await fetchAdditionalDetailsFromPosition(positionAddress, positionId);
    if (positionInfo) {
      inputEvent.token0Address = positionInfo.token0;
      inputEvent.token1Address = positionInfo.token1;
      inputEvent.tickLower = positionInfo.tickLower;
      inputEvent.tickUpper = positionInfo.tickUpper;
      inputEvent.poolFee = positionInfo.poolFee;
      console.log(`Position ${positionId} updated with additional details`);
    }
  }
  
  return output;
}
