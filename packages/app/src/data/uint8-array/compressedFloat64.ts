const EXPONENT_BITS = 11;
const FRACTION_BITS_IN_PART1 = 20;

const EXPONENT_MASK = (1 << EXPONENT_BITS) - 1;

const scratchDataView = new DataView(new ArrayBuffer(8));

function getFractionBytes(part1: number, part2: number): number {
  if (part2) {
    if (part2 & 0xfff) {
      return part2 & 0xf ? 7 : 6;
    } else if (part2 & 0xffff000) {
      return part2 & 0xff000 ? 5 : 4;
    } else {
      return 3;
    }
  } else {
    if (part1 & 0xfff) {
      return part1 & 0xf ? 3 : 2;
    } else {
      return part1 & 0xff000 ? 1 : 0;
    }
  }
}

/**
 * @param value
 * @returns the number bytes necessary to serialize the given value
 */
export function getCompressedFloat64ByteLength(value: number): number {
  scratchDataView.setFloat64(0, value);
  const part1 = scratchDataView.getUint32(0);
  const part2 = scratchDataView.getUint32(4);

  // the following code is duplicated from writeCompressedFloat64()...
  const exponent = (part1 >>> FRACTION_BITS_IN_PART1) & EXPONENT_MASK;
  // convert the exponent so that values close to 1023 are close to 0 to get better compression for common numbers
  const transformedExponent = exponent <= 1023 ? ((1023 - exponent) << 1) | 1 : (exponent - 1024) << 1;
  // we need 2 bytes for the "exponent part" if the exponent is longer than 3 bits
  const twoByteExponent = transformedExponent > 0b111;

  return (twoByteExponent ? 2 : 1) + getFractionBytes(part1, part2);
}

/**
 * Writes the given float64 number to the array starting at offset. Writes 1 to 9 bytes depending on the number.
 *
 * Use readCompressedFloat64() to read numbers written with this function.
 *
 * @returns the number of written bytes
 */
export function writeCompressedFloat64(array: Uint8Array, offset: number, value: number): number {
  scratchDataView.setFloat64(0, value);
  const part1 = scratchDataView.getUint32(0);
  const part2 = scratchDataView.getUint32(4);

  const sign = part1 >>> 31;
  const exponent = (part1 >>> FRACTION_BITS_IN_PART1) & EXPONENT_MASK;
  // convert the exponent so that values close to 1023 are close to 0 to get better compression for common numbers
  const transformedExponent = exponent <= 1023 ? ((1023 - exponent) << 1) | 1 : (exponent - 1024) << 1;
  // we need 2 bytes for the "exponent part" if the exponent is longer than 3 bits
  const twoByteExponent = transformedExponent > 0b111;

  const fractionBytes = getFractionBytes(part1, part2);

  const length = (twoByteExponent ? 2 : 1) + fractionBytes;
  if (offset < 0 || offset + length > array.length) {
    throw new RangeError("offset is out of bounds");
  }

  array[offset] =
    (twoByteExponent ? 0x80 : 0) |
    (sign << 6) |
    (fractionBytes << 3) |
    (twoByteExponent ? transformedExponent >>> 8 : transformedExponent);
  if (twoByteExponent) {
    array[offset + 1] = transformedExponent & 0xff;
  }

  if (fractionBytes > 0) {
    const fractionStart = offset + (twoByteExponent ? 2 : 1);
    // use switch with fall through
    switch (fractionBytes) {
      case 7:
        array[fractionStart + 6] = (part2 & 0xf) << 4;
      case 6:
        array[fractionStart + 5] = (part2 & 0xff0) >>> 4;
      case 5:
        array[fractionStart + 4] = (part2 & 0xff000) >>> 12;
      case 4:
        array[fractionStart + 3] = (part2 & 0xff00000) >>> 20;
      case 3:
        array[fractionStart + 2] = ((part1 & 0xf) << 4) | ((part2 & 0xf0000000) >>> 28);
      case 2:
        array[fractionStart + 1] = (part1 & 0xff0) >>> 4;
      case 1:
        array[fractionStart] = (part1 & 0xff000) >>> 12;
    }
  }

  return length;
}

/**
 * Reads a compressed float64 number written by writeCompressedFloat64().
 *
 * @param array
 * @param offset
 * @returns the read value and the number of bytes consumed (length)
 */
export function readCompressedFloat64(array: Uint8Array, offset: number): { value: number; length: number } {
  const byte0 = array[offset];
  const twoByteExponent = (byte0 & 0x80) > 0;
  const sign = byte0 & 0x40 ? 0x80000000 : 0;
  const fractionBytes = (byte0 >>> 3) & 0b111;

  let transformedExponent;
  if (twoByteExponent) {
    transformedExponent = ((byte0 & 0b111) << 8) | array[offset + 1];
  } else {
    transformedExponent = byte0 & 0b111;
  }
  const exponent = transformedExponent & 1 ? 1023 - (transformedExponent >>> 1) : (transformedExponent >>> 1) + 1024;

  let part1 = sign | (exponent << FRACTION_BITS_IN_PART1);
  let part2 = 0;

  if (fractionBytes > 0) {
    const fractionStart = offset + (twoByteExponent ? 2 : 1);
    // use switch with fall through
    switch (fractionBytes) {
      case 7:
        part2 |= array[fractionStart + 6] >>> 4;
      case 6:
        part2 |= array[fractionStart + 5] << 4;
      case 5:
        part2 |= array[fractionStart + 4] << 12;
      case 4:
        part2 |= array[fractionStart + 3] << 20;
      case 3:
        const fractionByte2 = array[fractionStart + 2];
        part2 |= fractionByte2 << 28;
        part1 |= fractionByte2 >>> 4;
      case 2:
        part1 |= array[fractionStart + 1] << 4;
      case 1:
        part1 |= array[fractionStart] << 12;
    }
  }

  scratchDataView.setUint32(0, part1);
  scratchDataView.setUint32(4, part2);
  return {
    value: scratchDataView.getFloat64(0),
    length: (twoByteExponent ? 2 : 1) + fractionBytes,
  };
}
