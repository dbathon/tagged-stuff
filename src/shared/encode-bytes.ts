const CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".split("");

const CHAR_TO_INDEX: Record<string, number> = {};
for (let i = 0; i < CHARS.length; ++i) {
  CHAR_TO_INDEX[CHARS[i]] = i;
}

function getPaddingBits(byteCount: number) {
  const bits = 6 - ((byteCount * 8) % 6);
  return bits === 6 ? 0 : bits;
}

/**
 * Encodes the bytes into a string.
 *
 * This basically uses base64, but the bytes are encoded as one large number, so the padding with zero-bits (if any) is
 * at the start and padding with "=" is never done.
 *
 * Doing the padding at the start is convenient, because it allows encoding 16 bytes (128 bits) into a 22 characters
 * string that never starts with "_" (ids starting with "_" are not allowed in jds...).
 *
 * @param bytes
 * @returns the encoded string
 */
export function encodeBytes(bytes: Uint8Array): string {
  const byteCount = bytes.length;

  let result = "";

  let currentValue = 0;
  // pad with zero-bits at the start
  let currentBits = getPaddingBits(byteCount);
  for (let i = 0; i < byteCount; ++i) {
    currentValue = (currentValue << 8) | bytes[i];
    currentBits += 8;

    while (currentBits >= 6) {
      currentBits -= 6;
      const index = currentValue >> currentBits;
      result += CHARS[index];
      currentValue -= index << currentBits;
    }
  }
  if (currentBits !== 0) {
    throw new Error("currentBits is not 0 unexpectedly");
  }
  return result;
}

/**
 * Decodes bytes that were encoded using encodeBytes().
 *
 * @param encodedBytes
 * @param resultArray optional, if given then it will be used to store the result and it will be returned
 * @returns the decoded bytes as a Uint8Array
 */
export function decodeBytes(encodedBytes: string, resultArray?: Uint8Array): Uint8Array {
  const encodedLength = encodedBytes.length;
  const byteCount = Math.floor((encodedLength * 6) / 8);
  if (resultArray !== undefined && resultArray.length < byteCount) {
    throw new Error("resultArray is not large enough");
  }
  const result = resultArray || new Uint8Array(byteCount);

  let paddingBits = getPaddingBits(byteCount);
  let currentValue = 0;
  let currentBits = 0;
  let nextByteIndex = 0;
  for (let i = 0; i < encodedLength; ++i) {
    const char = encodedBytes.charAt(i);
    const index = CHAR_TO_INDEX[char];
    if (char !== CHARS[index]) {
      throw new Error("invalid char '" + char + "' in encodedBytes");
    }
    currentValue = (currentValue << 6) | index;
    currentBits += 6;

    if (paddingBits > 0) {
      currentBits -= paddingBits;
      if (currentValue >> currentBits !== 0) {
        throw new Error("padding bits are not 0");
      }
      paddingBits = 0;
    }

    if (currentBits >= 8) {
      currentBits -= 8;
      const byte = currentValue >> currentBits;
      result[nextByteIndex++] = byte;
      currentValue -= byte << currentBits;
    }
  }

  if (currentBits !== 0 || nextByteIndex !== byteCount) {
    throw new Error("currentBits is not 0 unexpectedly or unexpected nextByteIndex");
  }

  return result;
}
