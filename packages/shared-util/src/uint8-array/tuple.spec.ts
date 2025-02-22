import { assert, expect, test } from "vitest";
import {
  getTupleByteLength,
  readTuple,
  tupleToUint8Array,
  type TupleType,
  type TupleTypeDefinition,
  writeTuple,
} from "./tuple";

function testTuple<T extends TupleTypeDefinition>(tupleType: T, values: TupleType<T>) {
  const length = getTupleByteLength(tupleType, values);
  const array = new Uint8Array(length + 2);

  expect(() => writeTuple(array, -1, tupleType, values)).toThrow("offset is out of bounds");
  expect(() => writeTuple(array, 3, tupleType, values)).toThrow("offset is out of bounds");
  expect(array).toEqual(new Uint8Array(length + 2));

  assert(writeTuple(array, 1, tupleType, values) === length);
  assert(array[0] === 0);
  assert(array.at(-1) === 0);

  const array2 = tupleToUint8Array(tupleType, values);
  expect(array2).toStrictEqual(array.subarray(1, length + 1));

  expect(readTuple(array, tupleType, 1)).toStrictEqual({ values, length });
  expect(readTuple(array2, tupleType)).toStrictEqual({ values, length });
}

test("tuple", () => {
  testTuple([] as const, []);
  testTuple(["string", "uint32"] as const, ["some string", 55]);
  testTuple(["string", "uint32", "string", "number", "number", "uint32raw", "array", "number", "number"] as const, [
    "string1",
    42,
    "string2",
    1,
    -42.2345,
    12345,
    Uint8Array.from([1, 2, 3, 4, 5]),
    NaN,
    -Infinity,
  ]);
});
