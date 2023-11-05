export function uint8ArrayToDataView(array: Uint8Array): DataView {
  return new DataView(array.buffer, array.byteOffset, array.byteLength);
}
