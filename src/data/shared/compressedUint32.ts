const LIMIT1 = 0b0011_1111;
const LIMIT2 = (LIMIT1 << 8) | 0xff;
const LIMIT3 = (LIMIT2 << 8) | 0xff;

function getCompressedUint32Length(uint32: number): 1 | 2 | 3 | 5 {
  if (uint32 >>> 0 !== uint32) {
    throw new Error("not a uint32: " + uint32);
  }
  if (uint32 <= LIMIT1) {
    return 1;
  }
  if (uint32 <= LIMIT2) {
    return 2;
  }
  if (uint32 <= LIMIT3) {
    return 3;
  }
  // we don't use length 4, because we encode the length in 2 bits, so there are only 4 options
  return 5;
}

/**
 * Writes the given uint32 to the array starting at offset. Writes 1, 2, 3 or 5 bytes depending on the uint32. The used
 * "encoding" is also "order preserving".
 *
 * Use readCompressedUint32() to read uint32s written with this function.
 *
 * @returns the number of written bytes
 */
export function writeCompressedUint32(array: Uint8Array, offset: number, uint32: number): 1 | 2 | 3 | 5 {
  const compressedLength = getCompressedUint32Length(uint32);
  let index = offset + compressedLength - 1;
  if (index >= array.length) {
    throw new Error("not enough space");
  }
  for (; index > offset; index--) {
    array[index] = uint32 & 0xff;
    uint32 >>>= 8;
  }
  const markerBits = compressedLength === 5 ? 3 : compressedLength - 1;
  array[offset] = uint32 | (markerBits << 6);

  return compressedLength;
}

/**
 * Reads a compressed uint32 written by writeCompressedUint32().
 *
 * @param array
 * @param offset
 * @returns the read uint32 and the number of bytes consumed (length)
 */
export function readCompressedUint32(array: Uint8Array, offset: number): { uint32: number; length: number } {
  const firstByte = array[offset];
  const markerBits = firstByte >> 6;
  const fullBytes = markerBits === 3 ? 4 : markerBits;
  let result = firstByte & LIMIT1;
  for (let i = 0; i < fullBytes; i++) {
    offset++;
    result = (result << 8) | array[offset];
  }
  return {
    uint32: result >>> 0,
    length: fullBytes + 1,
  };
}
