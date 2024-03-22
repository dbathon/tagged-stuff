import { expect, test } from "vitest";
import { compareJsonPrimitives } from "./compareJsonPrimitives";

test("compareJsonPrimitives", () => {
  const testCases: [unknown, unknown, -1 | 0 | 1][] = [
    [0, -1, 1],
    [0, 0, 0],
    [0, 1, -1],
    [0, "1", -1],
    [0, false, -1],
    [0, true, -1],
    [0, null, -1],
    [0, undefined, -1],
    [0, [], -1],
    [0, {}, -1],
    ["b", -1, 1],
    ["b", 0, 1],
    ["b", 1, 1],
    ["b", "a", 1],
    ["b", "b", 0],
    ["b", "c", -1],
    ["b", false, -1],
    ["b", true, -1],
    ["b", null, -1],
    ["b", undefined, -1],
    ["b", [], -1],
    ["b", {}, -1],
    [false, -1, 1],
    [false, 0, 1],
    [false, 1, 1],
    [false, "a", 1],
    [false, "b", 1],
    [false, "c", 1],
    [true, false, 1],
    [true, true, 0],
    [false, false, 0],
    [false, true, -1],
    [false, null, -1],
    [false, undefined, -1],
    [false, [], -1],
    [false, {}, -1],
    [null, -1, 1],
    [null, 0, 1],
    [null, 1, 1],
    [null, "a", 1],
    [null, "b", 1],
    [null, "c", 1],
    [null, false, 1],
    [null, true, 1],
    [null, null, 0],
    [null, undefined, -1],
    [null, [], -1],
    [null, {}, -1],
    ...[undefined, [], {}].flatMap((a: unknown) => {
      const result: [unknown, unknown, -1 | 0 | 1][] = [
        [a, -1, 1],
        [a, 0, 1],
        [a, 1, 1],
        [a, "a", 1],
        [a, "b", 1],
        [a, "c", 1],
        [a, false, 1],
        [a, true, 1],
        [a, null, 1],
        [a, undefined, 0],
        [a, [], 0],
        [a, {}, 0],
      ];
      return result;
    }),
  ];

  for (const [a, b, compareResult] of testCases) {
    expect(compareJsonPrimitives(a, b)).toBe(compareResult);
  }
});
