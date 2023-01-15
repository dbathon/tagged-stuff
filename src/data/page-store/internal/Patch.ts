const MAX_OFFSET = (1 << 16) - 1;

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
    if (bytes.length > 255) {
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

  applyTo(target: Uint8Array) {
    const length = this.bytes.length;
    for (let i = 0; i < length; i++) {
      target[this.offset + i] = this.bytes[i];
    }
  }
}
