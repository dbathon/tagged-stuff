// use a shared Uint8Array and DataView to avoid allocations
const scratchArray = new Uint8Array(4);
const scratchDataView = new DataView(scratchArray.buffer);

export function writeOrderPreservingFloat32(array: Uint8Array, offset: number, number: number): void {
  if (offset < 0 || offset + 4 > array.length) {
    throw new Error("offset is out of bounds");
  }
  if (number !== number) {
    // NaN: use one canonical serialization
    array[offset] = 0xff;
    array[offset + 1] = 0xff;
    array[offset + 2] = 0xff;
    array[offset + 3] = 0xff;
  }
  scratchDataView.setFloat32(0, number);
  if (scratchArray[0] > 0b0111_1111) {
    // number is negative: flip all bits
    array[offset] = ~scratchArray[0];
    array[offset + 1] = ~scratchArray[1];
    array[offset + 2] = ~scratchArray[2];
    array[offset + 3] = ~scratchArray[3];
  } else {
    // number is positive: set the first bit to one
    array[offset] = scratchArray[0] | 0b1000_0000;
    array[offset + 1] = scratchArray[1];
    array[offset + 2] = scratchArray[2];
    array[offset + 3] = scratchArray[3];
  }
}

/**
 * Reads numbers written with writeOrderPreservingFloat32().
 */
export function readOrderPreservingFloat32(array: Uint8Array, offset: number): number {
  if (offset < 0 || offset + 4 > array.length) {
    throw new Error("offset is out of bounds");
  }
  if (array[offset] <= 0b0111_1111) {
    scratchArray[offset] = ~array[offset];
    scratchArray[1] = ~array[offset + 1];
    scratchArray[2] = ~array[offset + 2];
    scratchArray[3] = ~array[offset + 3];
  } else {
    scratchArray[0] = array[offset] & 0b0111_1111;
    scratchArray[1] = array[offset + 1];
    scratchArray[2] = array[offset + 2];
    scratchArray[3] = array[offset + 3];
  }
  return scratchDataView.getFloat32(0);
}
