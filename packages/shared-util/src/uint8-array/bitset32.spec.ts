import { describe, assert, expect, test } from "vitest";
import { getBitset32ByteLength, readBitset32, writeBitset32 } from "./bitset32";

function xorShift32(x: number): number {
  /* Algorithm "xor" from p. 4 of Marsaglia, "Xorshift RNGs" */
  x ^= x << 13;
  x ^= x >>> 17;
  x ^= x << 5;
  return x >>> 0;
}

function newRandom(seed: number): () => number {
  let state = seed;
  return () => {
    const result = state;
    state = xorShift32(state);
    return result;
  };
}

function roundtripWithChecks(bits: number): { bits: number; length: number } {
  const array = Uint8Array.from([1, 1, 1, 1, 1, 1]);
  const expectedLength = getBitset32ByteLength(bits);
  assert(expectedLength >= 1);
  assert(expectedLength <= 5);

  const length = writeBitset32(array, 0, bits);
  assert(length === expectedLength);
  // does not modify the uint8 after the written bytes
  assert(array[length] === 1);

  const read = readBitset32(array, 0);
  assert(read.length === expectedLength);
  assert(read.bits === bits >>> 0);

  return read;
}

describe("read/writeBitset32()", () => {
  test("roundtrip works", () => {
    const testBitsets = [
      0,
      -1 >>> 0,
      1,
      1 << 9,
      1 << 18,
      1 << 27,
      1 | (0xff << 8),
      (1 << 9) | 0xff,
      (1 << 18) | 0xff,
      (1 << 27) | 0xff,
      934857,
      3487692,
      23765,
    ];

    // add some random numbers
    const random = newRandom(2);
    for (let i = 0; i < 1000; i++) {
      testBitsets.push(random());
    }

    for (const bitset of testBitsets) {
      const result = roundtripWithChecks(bitset);
      assert(result.bits === bitset);
    }
  });

  test("one byte cases", () => {
    for (let i = 0; i <= 0xf; i++) {
      let bits = 0;
      for (let j = 0; j < 4; j++) {
        if ((i & (1 << j)) !== 0) {
          bits |= 0xff << (j << 3);
        }
      }
      bits = bits >>> 0;
      const read = roundtripWithChecks(bits);
      assert(read.length === 1);
      assert(read.bits === bits);
    }
  });

  test("does not validate input and 'truncates' information", () => {
    for (const testValue of [-1, (-1 >>> 0) + 1, 2343.35345, -23546.345]) {
      const result = roundtripWithChecks(testValue);
      assert(result.bits === testValue >>> 0);
      expect(result.bits).not.toBe(testValue);
    }
  });

  test("throws on not enough space", () => {
    const array = new Uint8Array(2);
    expect(() => writeBitset32(array, 0, 0x101)).toThrowError("not enough space");
    expect(() => writeBitset32(array, 0, 0x100)).not.toThrowError();
    expect(() => writeBitset32(array, 1, 1)).toThrowError("not enough space");
    expect(() => writeBitset32(array, 2, 0)).toThrowError("not enough space");
    expect(() => writeBitset32(array, 3, 0)).toThrowError("not enough space");
  });

  test("throws on invalid first byte", () => {
    expect(() => readBitset32(Uint8Array.from([2, 0, 0, 0]), 0)).toThrowError("invalid first byte of encoded bits");
  });
});
