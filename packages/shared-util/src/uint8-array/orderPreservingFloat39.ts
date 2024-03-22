/**
 * The "float39" format serializes the first 39 bits of a float64 (including the full exponent and 27 bits of the
 * fraction) and one bit indicating whether the exact float64 can be recovered when reading. The serialized form takes
 * 5 bytes.
 *
 * This saves 3 bytes compared to always serializing the full float64 while still serializing the exact value in many
 * cases (in particular "small" integers).
 *
 * The serialization is also order preserving.
 */

// use a shared Uint8Array and DataView to avoid allocations
const scratchArray = new Uint8Array(8);
const scratchDataView = new DataView(scratchArray.buffer);

export function writeOrderPreservingFloat39(array: Uint8Array, offset: number, number: number): void {
  if (offset < 0 || offset + 5 > array.length) {
    throw new RangeError("offset is out of bounds");
  }
  if (number !== number) {
    // NaN: use one canonical serialization
    array[offset] = 0xff;
    array[offset + 1] = 0xff;
    array[offset + 2] = 0xff;
    array[offset + 3] = 0xff;
    // this is an "exact" representation, so the last bit needs to be 0
    array[offset + 4] = 0xfe;
    return;
  }
  scratchDataView.setFloat64(0, number);
  const exact = (scratchArray[4] & 1) === 0 && scratchArray[5] === 0 && scratchArray[6] === 0 && scratchArray[7] === 0;
  if (!exact) {
    // switch the last bit to 1
    scratchArray[4] |= 1;
  }
  if (scratchArray[0] > 0b0111_1111) {
    // number is negative: flip all bits
    array[offset] = ~scratchArray[0];
    array[offset + 1] = ~scratchArray[1];
    array[offset + 2] = ~scratchArray[2];
    array[offset + 3] = ~scratchArray[3];
    array[offset + 4] = ~scratchArray[4];
  } else {
    // number is positive: set the first bit to one
    array[offset] = scratchArray[0] | 0b1000_0000;
    array[offset + 1] = scratchArray[1];
    array[offset + 2] = scratchArray[2];
    array[offset + 3] = scratchArray[3];
    array[offset + 4] = scratchArray[4];
  }
}

/**
 * Reads numbers written with writeOrderPreservingFloat39().
 *
 * @returns the read value and whether it is the exact value that was written
 */
export function readOrderPreservingFloat39(array: Uint8Array, offset: number): { value: number; exact: boolean } {
  if (offset < 0 || offset + 5 > array.length) {
    throw new RangeError("offset is out of bounds");
  }
  // zero the last 4 bytes
  scratchDataView.setUint32(4, 0);

  if (array[offset] <= 0b0111_1111) {
    scratchArray[0] = ~array[offset];
    scratchArray[1] = ~array[offset + 1];
    scratchArray[2] = ~array[offset + 2];
    scratchArray[3] = ~array[offset + 3];
    scratchArray[4] = ~array[offset + 4];
  } else {
    scratchArray[0] = array[offset] & 0b0111_1111;
    scratchArray[1] = array[offset + 1];
    scratchArray[2] = array[offset + 2];
    scratchArray[3] = array[offset + 3];
    scratchArray[4] = array[offset + 4];
  }
  const exact = (scratchArray[4] & 1) === 0;
  if (!exact) {
    // switch the last bit to 0 before reading the float
    scratchArray[4] &= 0xfe;
  }
  return {
    value: scratchDataView.getFloat64(0),
    exact,
  };
}
