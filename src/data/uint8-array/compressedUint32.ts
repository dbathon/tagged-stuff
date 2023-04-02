function getFullBytes(uint32: number): number {
  if (uint32 >>> 0 !== uint32) {
    throw new TypeError("not a uint32: " + uint32);
  }
  if (uint32 <= 0x3fff) {
    return uint32 <= 0x3f ? 0 : 1;
  }
  if (uint32 <= 0x1fffffff) {
    return uint32 <= 0x3fffff ? 2 : 3;
  }
  return 4;
}

/**
 * Writes the given uint32 to the array starting at offset. Writes 1 to 5 bytes depending on the uint32. The used
 * "encoding" is also "order preserving".
 *
 * Use readCompressedUint32() to read uint32s written with this function.
 *
 * @returns the number of written bytes
 */
export function writeCompressedUint32(array: Uint8Array, offset: number, uint32: number): number {
  const fullBytes = getFullBytes(uint32);
  let index = offset + fullBytes;
  if (index >= array.length) {
    throw new RangeError("not enough space");
  }
  for (; index > offset; index--) {
    array[index] = uint32 & 0xff;
    uint32 >>>= 8;
  }
  const markerBits = fullBytes === 4 ? 0b111 : fullBytes << 1;
  array[offset] = uint32 | (markerBits << 5);

  return fullBytes + 1;
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
  const markerBits = firstByte >> 5;
  const fullBytes = markerBits === 0b111 ? 4 : markerBits >> 1;
  let result = firstByte & 0x3f;
  for (let i = 0; i < fullBytes; i++) {
    offset++;
    result = (result << 8) | array[offset];
  }
  return {
    uint32: result >>> 0,
    length: fullBytes + 1,
  };
}
