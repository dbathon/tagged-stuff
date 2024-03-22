export function uint8ArraysEqual(array1: Uint8Array, array2: Uint8Array): boolean {
  if (
    array1 === array2 ||
    (array1.buffer === array2.buffer &&
      array1.byteOffset === array2.byteOffset &&
      array1.byteLength === array2.byteLength)
  ) {
    return true;
  }

  const length = array1.length;
  if (length !== array2.length) {
    return false;
  }

  for (let i = 0; i < length; i++) {
    if (array1[i] !== array2[i]) {
      return false;
    }
  }
  return true;
}
