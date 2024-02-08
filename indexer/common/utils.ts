import {
  uint256, 
  Transaction
} from "../common/deps.ts";

export function formatFelt(key: string): string {
    return "0x" + BigInt(key).toString(16);
  }

export function formatU256(low: string, high: string): Number {
    return Number(uint256.uint256ToBN({ low: low, high: high }));
  }

export function formatI256(low: string, high: string, sign: string): Number {
    if(Number(sign) == 1) {
      return -formatU256(low, high);
    }
    return formatU256(low, high);
  }

export function formatI32(mag: string, sign: string): Number {
  if(Number(sign) == 1) {
    return -Number(mag);
  }
  return Number(mag);
}

export function senderAddress(tx: Transaction): string | undefined {
  if (tx.invokeV0) {
    return formatFelt(tx.invokeV0.contractAddress);
  } else if (tx.invokeV1) {
    return formatFelt(tx.invokeV1.senderAddress);
  } else if (tx.invokeV3) {
    return formatFelt(tx.invokeV3.senderAddress);
  } else if (tx.l1Handler) {
    return formatFelt(tx.l1Handler.contractAddress);
  } else {
    return undefined;
  }
}