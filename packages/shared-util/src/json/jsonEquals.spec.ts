import { assert, expect, test } from "vitest";
import { jsonEquals } from "./jsonEquals";

const testValues: unknown[] = [
  true,
  false,
  null,
  0,
  1,
  2,
  3,
  42,
  -1,
  "",
  "foo",
  [],
  [1, 2, 3],
  [true, false, null, 1, 2, 3, { a: 2 }],
  {},
  { a: true, b: 0, c: "" },
];

test("jsonEquals true", () => {
  for (const value of testValues) {
    assert(jsonEquals(value, value));

    const clone1 = JSON.parse(JSON.stringify(value));
    assert(jsonEquals(value, clone1));
    assert(jsonEquals(clone1, value));

    const clone2 = structuredClone(value);
    assert(jsonEquals(value, clone2));
    assert(jsonEquals(clone2, value));

    if (value && typeof value === "object" && !Array.isArray(value)) {
      const clonedObject = clone2 as Record<string, unknown>;

      // add a property with value undefined, it should then still equal
      clonedObject.extra = undefined;
      assert(jsonEquals(value, clonedObject));
      assert(jsonEquals(clonedObject, value));

      // but setting it to any if the other test values should then fail
      for (const value2 of testValues) {
        clonedObject.extra = value2;
        assert(!jsonEquals(value, clonedObject));
        assert(!jsonEquals(clonedObject, value));
      }
    }
  }
});

test("jsonEquals false", () => {
  for (let i = 0; i < testValues.length; i++) {
    for (let j = 0; j < testValues.length; j++) {
      if (i !== j) {
        assert(!jsonEquals(testValues[i], testValues[j]));
      }
    }
  }
});
