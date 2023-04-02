import { expect, test } from "vitest";
import { getTupleByteLength, readTuple, tupleToUint8Array, TupleType, TupleTypeDefinition, writeTuple } from "./tuple";

function testTuple<T extends TupleTypeDefinition>(tupleType: T, values: TupleType<T>) {
  const length = getTupleByteLength(tupleType, values);
  const array = new Uint8Array(length + 2);

  expect(() => writeTuple(array, -1, tupleType, values)).toThrow("offset is out of bounds");
  expect(() => writeTuple(array, 3, tupleType, values)).toThrow("offset is out of bounds");
  expect(array).toEqual(new Uint8Array(length + 2));

  expect(writeTuple(array, 1, tupleType, values)).toBe(length);
  expect(array[0]).toBe(0);
  expect(array.at(-1)).toBe(0);

  const array2 = tupleToUint8Array(tupleType, values);
  expect(array2).toStrictEqual(array.subarray(1, length + 1));

  expect(readTuple(array, tupleType, 1)).toStrictEqual({ values, length });
  expect(readTuple(array2, tupleType)).toStrictEqual({ values, length });
}

test("tuple", () => {
  testTuple([] as const, []);
  testTuple(["string", "uint32"] as const, ["some string", 55]);
  testTuple(["string", "uint32", "string", "number", "number", "number", "number"] as const, [
    "string1",
    42,
    "string2",
    1,
    -42.2345,
    NaN,
    -Infinity,
  ]);
});
