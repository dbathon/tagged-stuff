import { assert, expect, test } from "vitest";
import { uint8ArraysEqual } from "./uint8ArraysEqual";

function testEqual(array: ArrayLike<number>, prefix: ArrayLike<number>, expectedResult: boolean) {
  const result = uint8ArraysEqual(Uint8Array.from(array), Uint8Array.from(prefix));
  assert(result === expectedResult);
}

test("uint8ArraysEqual", () => {
  testEqual([1, 2, 3], [1, 2, 3], true);
  testEqual([1, 2, 3], [1, 2], false);
  testEqual([1, 2, 3], [1, 2, 3, 4], false);
  testEqual([1, 2, 3], [4, 2, 3], false);
  testEqual([1, 2, 3], [1, 4, 3], false);
  testEqual([1, 2, 3], [1, 2, 4], false);
  testEqual([1], [1], true);
  testEqual([1], [2], false);
  testEqual([], [], true);
  testEqual([], [42], false);

  const someArray = Uint8Array.from([1, 2, 3, 4]);
  assert(uint8ArraysEqual(someArray, someArray));

  assert(uint8ArraysEqual(someArray.subarray(0, 3), someArray.subarray(0, 3)));
  assert(!uint8ArraysEqual(someArray.subarray(0, 3), someArray.subarray(1, 4)));
  assert(!uint8ArraysEqual(someArray.subarray(0, 3), someArray.subarray(0, 2)));
  assert(!uint8ArraysEqual(someArray.subarray(0, 3), someArray.subarray(0, 4)));
});
