export function formatFelt(key: string): string {
    return "0x" + BigInt(key).toString(16);
  }
