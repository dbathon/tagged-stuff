import { assert, expect, test } from "vitest";
import { isPrefixOfUint8Array } from "./isPrefixOfUint8Array";

function testPrefix(array: ArrayLike<number>, prefix: ArrayLike<number>, expectedResult: boolean) {
  const result = isPrefixOfUint8Array(Uint8Array.from(array), Uint8Array.from(prefix));
  assert(result === expectedResult);
}

test("isPrefixOfUint8Array", () => {
  testPrefix([1, 2, 3], [], true);
  testPrefix([1, 2, 3], [1], true);
  testPrefix([1, 2, 3], [1, 2], true);
  testPrefix([1, 2, 3], [1, 2, 3], true);
  testPrefix([1, 2, 3], [1, 2, 3, 4], false);
  testPrefix([1, 2, 3], [3], false);
  testPrefix([1, 2, 3], [1, 3], false);
  testPrefix([1, 2, 3], [1, 3, 3], false);

  const someArray = Uint8Array.from([1]);
  assert(isPrefixOfUint8Array(someArray, someArray) === true);
});
