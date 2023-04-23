const MAX_OFFSET = (1 << 16) - 1;
const MAX_LENGTH = 255;

/**
 * Represents a binary patch in a page.
 *
 * The serialized form starts with the offset as a uint16 and then the length as a uint8 and then the bytes.
 * TODO: maybe we can optimize this in the future, but it should be good enough for now.
 */
export class Patch {
  constructor(readonly offset: number, readonly bytes: Uint8Array) {
    if (offset < 0 || offset > MAX_OFFSET) {
      throw new RangeError("invalid offset: " + offset);
    }
    if (bytes.length > MAX_LENGTH) {
      throw new RangeError("too many bytes: " + bytes.length);
    }
    const a: ArrayLike<number> = bytes;
  }

  /**
   * Assumes that source never changes, the resulting patch will use the underlying buffer of source for its bytes.
   */
  static deserialize(source: DataView, offset: number): Patch {
    const patchOffset = source.getUint16(offset);
    const patchLength = source.getUint8(offset + 2);
    return new Patch(patchOffset, new Uint8Array(source.buffer, source.byteOffset + offset + 3, patchLength));
  }

  static createPatches(oldBytes: { [n: number]: number }, newBytes: number[] | Uint8Array, length: number): Patch[] {
    const result: Patch[] = [];
    for (let i = 0; i < length; i++) {
      if (newBytes[i] !== oldBytes[i]) {
        const start = i;
        // check a few bytes past the last difference: having one patch with a few identical bytes can still be shorter than multiple patches
        let lastDifferent = i;
        let patchLength = 1;
        while (i - lastDifferent <= 3 && patchLength < MAX_LENGTH && i + 1 < length) {
          i++;
          patchLength++;
          if (newBytes[i] !== oldBytes[i]) {
            lastDifferent = i;
          }
        }
        result.push(new Patch(start, Uint8Array.from(newBytes.slice(start, lastDifferent + 1))));
      }
    }
    return result;
  }

  /**
   * @returns an "optimized" list of patches without overlaps or duplications
   */
  static mergePatches(patches: Patch[]): Patch[] {
    if (patches.length <= 1) {
      return patches;
    }

    // simple implementation, that just applies all patches to an array and then builds new patches...
    const bytes: number[] = [];
    for (const patch of patches) {
      patch.applyTo(bytes);
    }

    return Patch.createPatches([], bytes, bytes.length);
  }

  static patchesEqual(patches1: Patch[] | undefined, patches2: Patch[] | undefined): boolean {
    if (patches1 === patches2) {
      return true;
    }
    if (patches1 && patches2 && patches1.length === patches2.length) {
      // both are arrays of equal length
      for (let i = 0; i < patches1.length; i++) {
        if (!patches1[i].equals(patches2[i])) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  get serializedLength(): number {
    return this.bytes.length + 3;
  }

  serialize(target: DataView, offset: number) {
    if (offset + this.serializedLength > target.byteLength) {
      throw new Error("not enough capacity in target");
    }

    target.setUint16(offset, this.offset);
    const length = this.bytes.length;
    target.setUint8(offset + 2, length);
    for (let i = 0; i < length; i++) {
      target.setUint8(offset + 3 + i, this.bytes[i]);
    }
  }

  applyTo(target: { [n: number]: number }) {
    const length = this.bytes.length;
    for (let i = 0; i < length; i++) {
      target[this.offset + i] = this.bytes[i];
    }
  }

  equals(other: Patch): boolean {
    const bytes1 = this.bytes;
    const bytes2 = other.bytes;
    const length = bytes1.length;
    if (this.offset !== other.offset || length !== bytes2.length) {
      return false;
    }
    for (let i = 0; i < length; i++) {
      if (bytes1[i] !== bytes2[i]) {
        return false;
      }
    }
    return true;
  }
}
