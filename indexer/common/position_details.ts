import { Contract, RpcProvider } from "./deps.ts";
import { nftPositionAbi } from "../abi/nft_router.ts"
import { formatBigIntToAddress, formatI32 } from "./utils.ts";

export type PositionInfo = {
  token0: string;
  token1: string;
  tickLower: Number;
  tickUpper: Number;
  poolFee: Number;
};

type Tick = {
  mag: BigInt;
  sign: boolean;
};

export async function fetchAdditionalDetailsFromPosition(
  positionAddress: string,
  positionId: Number
): Promise<PositionInfo | undefined> {
  const contract = positionContract(positionAddress);

  const output =
    await Promise.all([
      safeCall<{ output: Array<string> }>(contract.get_position(positionId), { output: undefined }),
  ]);
  const { tick_lower, tick_upper } = output[0]["0"]
  const { token0, token1, fee } = output[0]["1"]

  if (!token0 || !token1) {
    console.log('Tokens not found')
    return undefined;
  }

  return {
    token0: formatBigIntToAddress(token0),
    token1: formatBigIntToAddress(token1),
    tickLower: formatTick(tick_lower),
    tickUpper: formatTick(tick_upper),
    poolFee: Number(fee),
  };
}

async function safeCall<T extends object>(
  pr: Promise<T>,
  defaultValue: Partial<T>,
): Promise<Partial<T>> {
  try {
    return await pr;
  } catch (err) {
    if (
      err.message.includes(
        "Custom Hint Error: Entry point EntryPointSelector",
      ) ||
      err.message.includes("Invalid message selector") ||
      err.message.includes("Contract not found")
    ) {
      return defaultValue;
    }
    throw err;
  }
}

const provider = new RpcProvider({
  nodeUrl: Deno.env.get("RPC_URL"),
});
  
function positionContract(positionAddress: string) {
  return new Contract(nftPositionAbi, positionAddress, provider);
}

function formatTick(tick: Tick) {
  const sign = tick.sign ? '1' : '0';
  return formatI32(tick.mag.toString(), sign);
}