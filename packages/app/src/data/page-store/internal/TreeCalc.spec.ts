import { expect, test } from "vitest";
import { TreeCalc } from "./TreeCalc";

test("TreeCalc", () => {
  const calc8K = new TreeCalc(1 << 13, 6, -1 >>> 0);
  const calc16K = new TreeCalc(1 << 14, 6, -1 >>> 0);

  expect(calc8K.height).toBe(4);
  expect(calc16K.height).toBe(3);

  // TODO

  console.log(calc8K.getPath(2));
  console.log(calc16K.getPath(1));
  console.log(calc8K.getPath(calc8K.maxPageNumber));
  console.log(calc16K.getPath(calc16K.maxPageNumber));
});
