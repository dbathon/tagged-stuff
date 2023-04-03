import { describe, expect, test } from "vitest";
import { getCompressedFloat64ByteLength, readCompressedFloat64, writeCompressedFloat64 } from "./compressedFloat64";

describe("read/writeCompressedFloat64()", () => {
  test("throws on offset is out of bounds", () => {
    const array = new Uint8Array(9);
    const zeroArray = new Uint8Array(9);
    const tests: [number, number, boolean?][] = [
      [9, 32, true],
      [10, 32, true],
      [-1, 32, true],
      [8, 32],
      [7, 320],
      [6, 320000],
      [5, 32000000000],
    ];
    for (const [offset, value, skipMinusOne] of tests) {
      array.set(zeroArray);
      expect(() => writeCompressedFloat64(array, offset, value)).toThrowError("offset is out of bounds");
      // no modification
      expect(array).toEqual(zeroArray);

      if (!skipMinusOne) {
        expect(() => writeCompressedFloat64(array, offset - 1, value)).not.toThrowError();
      }
    }
  });

  const testValues = [
    0,
    1,
    2,
    3,
    4,
    8,
    16,
    24,
    100,
    250,
    600,
    2000,
    56789,
    2309543,
    1.5,
    1.2,
    1.23,
    1.234,
    1.2345,
    1.23456,
    1.234567,
    1.2345678,
    1.23456789,
    1.234567891,
    1.2345678912,
    1.23456789123,
    1.234567891234,
    1.2345678912345,
    1.23456789123456,
    1.234567891234567,
    1.2345678912345678,
    1.23456789123456789,
    1.234567891234567891,
    1.234567891234567891,
    12.34567891234567891,
    123.4567891234567891,
    1234.567891234567891,
    12345.67891234567891,
    123456.7891234567891,
    1234567.891234567891,
    12345678.91234567891,
    123456789.1234567891,
    1234567891.234567891,
    12345678912.34567891,
    123456789123.4567891,
    1234567891234.567891,
    12345678912345.67891,
    123456789123456.7891,
    1234567891234567.891,
    12345678912345678.91,
    123456789123456789.1,
    1234567891234567891.0,
    1234567891234567891e10,
    1234567891234567891e20,
    1234567891234567891e40,
    1234567891234567891e80,
    1234567891234567891e160,
    1234567891234567891e300,
    1234567891234567891e-10,
    1234567891234567891e-20,
    1234567891234567891e-40,
    1234567891234567891e-80,
    1234567891234567891e-160,
    1234567891234567891e-300,
    NaN,
  ];

  function testRoundtrip(testValue: number) {
    const array = new Uint8Array(11);
    const writeLength = writeCompressedFloat64(array, 1, testValue);
    expect(writeLength).toBeGreaterThanOrEqual(1);
    expect(writeLength).toBeLessThanOrEqual(9);

    // check that other array values are unchanged
    expect(array[0]).toBe(0);
    for (let i = writeLength + 1; i < array.length; i++) {
      expect(array[i]).toBe(0);
    }

    const { value: readValue, length: readLength } = readCompressedFloat64(array, 1);
    expect(readValue).toBe(testValue);
    expect(readLength).toBe(writeLength);

    expect(getCompressedFloat64ByteLength(testValue)).toBe(writeLength);
  }

  test("reads the written value", () => {
    for (const testValue of testValues) {
      testRoundtrip(testValue);
      testRoundtrip(-testValue);
    }
  });

  test("all exponents", () => {
    const dataView = new DataView(new ArrayBuffer(8));
    for (let exponent = 0; exponent < 2048; exponent++) {
      const part1 = exponent << 20;
      dataView.setUint32(0, part1);
      testRoundtrip(dataView.getFloat64(0));
      dataView.setUint32(0, part1 | (1 << 19));
      testRoundtrip(dataView.getFloat64(0));
      dataView.setUint32(0, part1 | 0x80000000);
      testRoundtrip(dataView.getFloat64(0));
      dataView.setUint32(0, part1 | 0x80000000 | (1 << 19));
      testRoundtrip(dataView.getFloat64(0));
    }
  });
});
