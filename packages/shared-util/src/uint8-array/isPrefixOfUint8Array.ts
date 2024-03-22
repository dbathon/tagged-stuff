export function isPrefixOfUint8Array(array: Uint8Array, prefix: Uint8Array): boolean {
  if (array === prefix) {
    return true;
  }
  const length = prefix.length;
  if (length > array.length) {
    return false;
  }
  for (let i = 0; i < length; i++) {
    if (array[i] !== prefix[i]) {
      return false;
    }
  }
  return true;
}
