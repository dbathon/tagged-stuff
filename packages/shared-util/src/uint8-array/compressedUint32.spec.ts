import { describe, assert, expect, test } from "vitest";
import { compareUint8Arrays } from "./compareUint8Arrays";
import { getCompressedUint32ByteLength, readCompressedUint32, writeCompressedUint32 } from "./compressedUint32";

describe("read/writeCompressedUint32()", () => {
  test("throws on invalid unit32", () => {
    const array = new Uint8Array(10);
    [-1, 1.234, (-1 >>> 0) + 1].forEach((n) =>
      expect(() => writeCompressedUint32(array, 0, n)).toThrowError("not a uint32")
    );
  });

  test("throws on not enough space", () => {
    const array = new Uint8Array(5);
    const zeroArray = new Uint8Array(5);
    const tests: [number, number][] = [
      [1, 0xffffffff],
      [1, 0x2fffffff],
      [2, 0x1fffffff],
      [2, 0xffffff],
      [2, 0x4fffff],
      [3, 0x3fffff],
      [4, 0x3fff],
      [5, 0x0],
    ];
    for (const [offset, uint32] of tests) {
      array.set(zeroArray);
      expect(() => writeCompressedUint32(array, offset, uint32)).toThrowError("not enough space");
      // no modification
      expect(array).toEqual(zeroArray);

      expect(() => writeCompressedUint32(array, offset - 1, uint32)).not.toThrowError("not enough space");
    }
  });

  const testValues = [
    0,
    1,
    2,
    3,
    0xf,
    0xff,
    0xfff,
    0xffff,
    0xfffff,
    0xffffff,
    0xfffffff,
    0xffffffff,
    0b10101010_10101010_10101010_10101010,
    0b01010101_01010101_01010101_01010101,
    0xf0f0f0f0,
    0x0f0f0f0f,
    0x3f,
    0x3fff,
    0x3fffff,
    0x1fffffff,
    0x3f + 1,
    0x3fff + 1,
    0x3fffff + 1,
    0x1fffffff + 1,
  ].sort((a, b) => a - b);

  test("reads the written value", () => {
    const array = new Uint8Array(7);
    const zeroArray = new Uint8Array(7);
    for (const testValue of testValues) {
      array.set(zeroArray);
      const writeLength = writeCompressedUint32(array, 1, testValue);
      assert(writeLength >= 1);
      assert(writeLength <= 5);

      // check that other array values are unchanged
      assert(array[0] === 0);
      for (let i = writeLength + 1; i < array.length; i++) {
        assert(array[i] === 0);
      }

      const { uint32: readValue, length: readLength } = readCompressedUint32(array, 1);
      assert(readValue === testValue);
      assert(readLength === writeLength);

      assert(getCompressedUint32ByteLength(testValue) === writeLength);
    }
  });

  test("is order preserving", () => {
    function testPreserving(value: number, largerValue: number) {
      const a = new Uint8Array(5);
      const b = new Uint8Array(5);
      writeCompressedUint32(a, 0, value);
      writeCompressedUint32(b, 0, largerValue);
      expect(value).toBeLessThan(largerValue);
      assert(compareUint8Arrays(a, b) === -1);
    }

    for (let i = 1; i < testValues.length; i++) {
      testPreserving(testValues[i - 1], testValues[i]);
      for (let j = 1; j < 10; j++) {
        const value = testValues[i] - j;
        if (value >= 0) {
          testPreserving(value, testValues[i]);
        }
      }
    }
  });

  test("non-default reads", () => {
    function testRead(bytes: number[], expected: number, expectedLength: number) {
      const array = Uint8Array.from(bytes);
      const result = readCompressedUint32(array, 0);
      assert(result.uint32 === expected);
      assert(result.length === expectedLength);
    }

    // inputs that are too short are extended with 0...
    testRead([], 0, 1);
    testRead([0b01000000], 0, 2);
    testRead([0b10000000], 0, 3);
    testRead([0b11000000], 0, 4);
    testRead([0b11100000], 0, 5);
    testRead([0b10000111, 0x34], 0x73400, 3);

    // numbers that are encoded in too many bytes
    testRead([0b01000000, 1], 1, 2);
    testRead([0b10000000, 0, 2], 2, 3);
    testRead([0b11000000, 0, 0, 3], 3, 4);
    testRead([0b11100000, 0, 0, 0, 4], 4, 5);
  });
});
