import { murmurHash3_x86_32 } from "../misc/murmurHash3";
import { uint8ArraysEqual } from "./uint8ArraysEqual";

/**
 * Assumes that added arrays don't change (they are not copied)...
 */
export class Uint8ArraySet {
  private entriesByHash = new Map<number, Uint8Array[]>();
  private _size = 0;

  get size(): number {
    return this._size;
  }

  clear(): void {
    this.entriesByHash.clear();
    this._size = 0;
  }

  add(array: Uint8Array, hashIfAvailable?: number): boolean {
    const hash = hashIfAvailable ?? murmurHash3_x86_32(array);
    const entries = this.entriesByHash.get(hash);
    if (!entries) {
      this.entriesByHash.set(hash, [array]);
      ++this._size;
      return true;
    }
    for (const entry of entries) {
      if (uint8ArraysEqual(entry, array)) {
        return false;
      }
    }
    entries.push(array);
    ++this._size;
    return true;
  }

  has(array: Uint8Array, hashIfAvailable?: number): boolean {
    const hash = hashIfAvailable ?? murmurHash3_x86_32(array);
    const entries = this.entriesByHash.get(hash);
    if (entries) {
      for (const entry of entries) {
        if (uint8ArraysEqual(entry, array)) {
          return true;
        }
      }
    }
    return false;
  }

  delete(array: Uint8Array, hashIfAvailable?: number): boolean {
    const hash = hashIfAvailable ?? murmurHash3_x86_32(array);
    const entries = this.entriesByHash.get(hash);
    if (entries) {
      const entriesLength = entries.length;
      for (let i = 0; i < entriesLength; i++) {
        if (uint8ArraysEqual(entries[i], array)) {
          if (entriesLength === 1) {
            this.entriesByHash.delete(hash);
          } else {
            entries.splice(i, 1);
          }
          --this._size;
          return true;
        }
      }
    }
    return false;
  }

  forEach(callbackFn: (array: Uint8Array, hash: number) => void): void {
    this.entriesByHash.forEach((entries, hash) => {
      for (const entry of entries) {
        callbackFn(entry, hash);
      }
    });
  }
}
