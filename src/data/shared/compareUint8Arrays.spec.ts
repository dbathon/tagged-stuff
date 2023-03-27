import { expect, test } from "vitest";
import { compareUint8Arrays } from "./compareUint8Arrays";

/** a is expected to be before b. */
function testCompare(a: number[], b: number[]) {
  const aArray = Uint8Array.from(a);
  const bArray = Uint8Array.from(b);
  expect(compareUint8Arrays(aArray, bArray)).toBe(-1);
  expect(compareUint8Arrays(bArray, aArray)).toBe(1);
  expect(compareUint8Arrays(aArray, aArray)).toBe(0);
  expect(compareUint8Arrays(bArray, bArray)).toBe(0);

  if (a.length < 20) {
    // also test with "extended" arrays
    testCompare([...a, 0], [...b, 0]);
  }
}

test("compare", () => {
  testCompare([], [0]);
  testCompare([0], [0, 0]);
  testCompare([0, 0, 1], [0, 1]);
  testCompare([254, 1], [255]);
});
