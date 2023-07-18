function requiredBytes(byte: number): number {
  return byte !== 0 && byte !== 0xff ? 1 : 0;
}

/**
 * @param bits
 * @returns the number of bytes that will be used by writeBitset32() to write the given bits
 */
export function getBitset32ByteLength(bits: number): number {
  // convert bits to uint32, technically unnecessary, but might allow better optimization
  bits = bits >>> 0;
  return (
    1 +
    requiredBytes(bits & 0xff) +
    requiredBytes((bits >>> 8) & 0xff) +
    requiredBytes((bits >>> 16) & 0xff) +
    requiredBytes((bits >>> 24) & 0xff)
  );
}

/**
 * Writes the lowest 32 bits of the given number to the array starting at offset. Writes 1 to 5 bytes depending on the
 * bits.
 *
 * The used "encoding" is "optimized" for "bitset" values where often only a few or almost all bits are set:
 * * bitsets where full bytes are set or not set (including no bits set and all bits set) will only use one byte
 * * if only one bit is set (no matter where), then only two bytes are used
 *
 * Use readBitset32() to read bits written with this function.
 *
 * @param array
 * @param offset
 * @param bits
 * @returns the number of written bytes
 */
export function writeBitset32(array: Uint8Array, offset: number, bits: number): number {
  // convert bits to uint32, technically unnecessary, but might allow better optimization
  bits = bits >>> 0;
  let length = 1;
  let byteZero = 0;

  for (let i = 0; i < 8; i += 2) {
    const byte = (bits >>> (i << 2)) & 0xff;
    if (byte === 0xff) {
      byteZero |= 0b11 << i;
    } else if (byte > 0) {
      byteZero |= 0b01 << i;
      array[offset + length] = byte;
      length++;
    } else {
      // nothing to do, byte === 0, basically: byteZero |= 0b00 << (i << 1);
    }
  }

  if (offset + length > array.length) {
    // we have potentially already modified array, but that should be okay...
    throw new RangeError("not enough space");
  }

  array[offset] = byteZero;
  return length;
}

/**
 * Reads bits written by writeBitset32().
 *
 * @param array
 * @param offset
 * @returns the read bits as a "uint32" and the number of bytes consumed (length)
 */
export function readBitset32(array: Uint8Array, offset: number): { bits: number; length: number } {
  let bits = 0;
  let length = 1;
  const byteZero = array[offset];

  for (let i = 0; i < 8; i += 2) {
    let byte;
    switch ((byteZero >>> i) & 0b11) {
      case 0b11:
        byte = 0xff;
        break;
      case 0b01:
        byte = array[offset + length];
        length++;
        break;
      case 0b00:
        byte = 0;
        break;
      default:
        // 0b10 is unused...
        throw new Error("invalid first byte of encoded bits");
    }

    bits |= byte << (i << 2);
  }

  return { bits: bits >>> 0, length };
}
