import { describe, expect, test } from "vitest";
import { compareUint8Arrays } from "./compareUint8Arrays";
import { readCompressedUint32, writeCompressedUint32 } from "./compressedUint32";

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
      [1, 0xffffff],
      [3, 0xfffff],
      [4, 0xfff],
      [5, 0x0],
    ];
    for (const [offset, uint32] of tests) {
      expect(() => writeCompressedUint32(array, offset, uint32)).toThrowError("not enough space");
      // no modification
      expect(array).toEqual(zeroArray);
    }
  });

  const testValues = [
    0, 1, 2, 3, 0xf, 0xff, 0xfff, 0xffff, 0xfffff, 0xffffff, 0xfffffff, 0xffffffff,
    0b10101010_10101010_10101010_10101010, 0b01010101_01010101_01010101_01010101, 0xf0f0f0f0, 0x0f0f0f0f,
  ];
  test("reads the written value", () => {
    const array = new Uint8Array(7);
    const zeroArray = new Uint8Array(7);
    for (const testValue of testValues) {
      array.set(zeroArray);
      const writeLength = writeCompressedUint32(array, 1, testValue);
      expect(writeLength).toBeGreaterThanOrEqual(1);
      expect(writeLength).toBeLessThanOrEqual(5);

      // check that other array values are unchanged
      expect(array[0]).toBe(0);
      for (let i = writeLength + 1; i < array.length; i++) {
        expect(array[i]).toBe(0);
      }

      const { uint32: readValue, length: readLength } = readCompressedUint32(array, 1);
      expect(readValue).toBe(testValue);
      expect(readLength).toBe(writeLength);
    }
  });

  test("is order preserving", () => {
    function testPreserving(value: number, largerValue: number) {
      const a = new Uint8Array(5);
      const b = new Uint8Array(5);
      writeCompressedUint32(a, 0, value);
      writeCompressedUint32(b, 0, largerValue);
      expect(value).toBeLessThan(largerValue);
      expect(compareUint8Arrays(a, b)).toBe(-1);
    }

    const sortedTestValues = [...testValues].sort((a, b) => a - b);
    for (let i = 1; i < sortedTestValues.length; i++) {
      testPreserving(sortedTestValues[i - 1], sortedTestValues[i]);
      for (let j = 1; j < 10; j++) {
        const value = sortedTestValues[i] - j;
        if (value >= 0) {
          testPreserving(value, sortedTestValues[i]);
        }
      }
    }
  });

  test("non-default reads", () => {
    function testRead(bytes: number[], expected: number, expectedLength: number) {
      const array = Uint8Array.from(bytes);
      const result = readCompressedUint32(array, 0);
      expect(result.uint32).toBe(expected);
      expect(result.length).toBe(expectedLength);
    }

    // inputs that are too short are extended with 0...
    testRead([], 0, 1);
    testRead([0b01000000], 0, 2);
    testRead([0b10000000], 0, 3);
    testRead([0b11000000], 0, 5);
    testRead([0b10000111, 0x34], 0x73400, 3);

    // numbers that are encoded in too many bytes
    testRead([0b01000000, 1], 1, 2);
    testRead([0b10000000, 0, 2], 2, 3);
    testRead([0b11000000, 0, 0, 0, 4], 4, 5);
  });
});
