export function compareUint8Arrays(array1: Uint8Array, array2: Uint8Array): -1 | 0 | 1 {
  if (
    array1 === array2 ||
    (array1.buffer === array2.buffer &&
      array1.byteOffset === array2.byteOffset &&
      array1.byteLength === array2.byteLength)
  ) {
    return 0;
  }
  const length = Math.min(array1.length, array2.length);
  for (let i = 0; i < length; i++) {
    const diff = array1[i] - array2[i];
    if (diff < 0) {
      return -1;
    }
    if (diff > 0) {
      return 1;
    }
  }
  const after1: number | undefined = array1[length];
  const after2: number | undefined = array2[length];
  if (after1 !== after2) {
    // one of them must be undefined
    return after1 === undefined ? -1 : 1;
  }
  return 0;
}
