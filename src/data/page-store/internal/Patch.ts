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
      throw new Error("invalid offset: " + offset);
    }
    if (bytes.length > MAX_LENGTH) {
      throw new Error("too many bytes: " + bytes.length);
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

    const result: Patch[] = [];
    for (let i = 0; i < bytes.length; i++) {
      if (bytes[i] !== undefined) {
        const start = i;
        while (bytes[i + 1] !== undefined && i + 2 - start <= MAX_LENGTH) {
          i++;
        }
        const end = i + 1;
        result.push(new Patch(start, Uint8Array.from(bytes.slice(start, end))));
      }
    }

    return result;
  }

  get serializedLength(): number {
    return this.bytes.length + 3;
  }

  serialize(target: DataView, offset: number) {
    if (offset + this.serializedLength >= target.byteLength) {
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
}
