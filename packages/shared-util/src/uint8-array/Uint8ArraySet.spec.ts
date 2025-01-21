import { assert, expect, test } from "vitest";
import { Uint8ArraySet } from "./Uint8ArraySet";

test("Uint8ArraySet", () => {
  const testSet = new Uint8ArraySet();

  assert(testSet.size === 0);

  const a1 = Uint8Array.from([1, 2, 3]);
  const a2 = Uint8Array.from([2, 3, 4]);

  for (const set of [testSet, testSet.copy()]) {
    assert(set.has(a1) === false);
    assert(set.has(a2) === false);
    assert(set.delete(a1) === false);
    assert(set.delete(a2) === false);
    assert(set.size === 0);
  }

  assert(testSet.add(a1) === true);
  assert(testSet.add(a1) === false);
  for (const set of [testSet, testSet.copy()]) {
    assert(set.has(a1) === true);
    assert(set.has(a2) === false);
    assert(set.has(Uint8Array.from(a1)) === true);
    assert(set.has(Uint8Array.from(a2)) === false);
    assert(set.size === 1);
  }

  assert(testSet.add(a2) === true);
  assert(testSet.add(a2) === false);
  assert(testSet.add(a1) === false);
  for (const set of [testSet, testSet.copy()]) {
    assert(set.has(a1) === true);
    assert(set.has(a2) === true);
    assert(set.has(Uint8Array.from(a1)) === true);
    assert(set.has(Uint8Array.from(a2)) === true);
    assert(set.size === 2);
  }

  assert(testSet.delete(a2) === true);
  assert(testSet.delete(a2) === false);
  for (const set of [testSet, testSet.copy()]) {
    assert(set.has(a1) === true);
    assert(set.has(a2) === false);
    assert(set.has(Uint8Array.from(a1)) === true);
    assert(set.has(Uint8Array.from(a2)) === false);
    assert(set.size === 1);
  }

  assert(testSet.delete(Uint8Array.from(a1)) === true);
  assert(testSet.delete(a1) === false);
  for (const set of [testSet, testSet.copy()]) {
    assert(set.has(a1) === false);
    assert(set.has(a2) === false);
    assert(set.has(Uint8Array.from(a1)) === false);
    assert(set.has(Uint8Array.from(a2)) === false);
    assert(set.size === 0);
  }
});
