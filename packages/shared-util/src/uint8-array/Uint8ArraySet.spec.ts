import { expect, test } from "vitest";
import { Uint8ArraySet } from "./Uint8ArraySet";

test("Uint8ArraySet", () => {
  const testSet = new Uint8ArraySet();

  expect(testSet.size).toBe(0);

  const a1 = Uint8Array.from([1, 2, 3]);
  const a2 = Uint8Array.from([2, 3, 4]);

  for (const set of [testSet, testSet.copy()]) {
    expect(set.has(a1)).toBe(false);
    expect(set.has(a2)).toBe(false);
    expect(set.delete(a1)).toBe(false);
    expect(set.delete(a2)).toBe(false);
    expect(set.size).toBe(0);
  }

  expect(testSet.add(a1)).toBe(true);
  expect(testSet.add(a1)).toBe(false);
  for (const set of [testSet, testSet.copy()]) {
    expect(set.has(a1)).toBe(true);
    expect(set.has(a2)).toBe(false);
    expect(set.has(Uint8Array.from(a1))).toBe(true);
    expect(set.has(Uint8Array.from(a2))).toBe(false);
    expect(set.size).toBe(1);
  }

  expect(testSet.add(a2)).toBe(true);
  expect(testSet.add(a2)).toBe(false);
  expect(testSet.add(a1)).toBe(false);
  for (const set of [testSet, testSet.copy()]) {
    expect(set.has(a1)).toBe(true);
    expect(set.has(a2)).toBe(true);
    expect(set.has(Uint8Array.from(a1))).toBe(true);
    expect(set.has(Uint8Array.from(a2))).toBe(true);
    expect(set.size).toBe(2);
  }

  expect(testSet.delete(a2)).toBe(true);
  expect(testSet.delete(a2)).toBe(false);
  for (const set of [testSet, testSet.copy()]) {
    expect(set.has(a1)).toBe(true);
    expect(set.has(a2)).toBe(false);
    expect(set.has(Uint8Array.from(a1))).toBe(true);
    expect(set.has(Uint8Array.from(a2))).toBe(false);
    expect(set.size).toBe(1);
  }

  expect(testSet.delete(Uint8Array.from(a1))).toBe(true);
  expect(testSet.delete(a1)).toBe(false);
  for (const set of [testSet, testSet.copy()]) {
    expect(set.has(a1)).toBe(false);
    expect(set.has(a2)).toBe(false);
    expect(set.has(Uint8Array.from(a1))).toBe(false);
    expect(set.has(Uint8Array.from(a2))).toBe(false);
    expect(set.size).toBe(0);
  }
});
