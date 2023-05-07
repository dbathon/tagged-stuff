import { expect, test } from "vitest";
import { Uint8ArraySet } from "./Uint8ArraySet";

test("Uint8ArraySet", () => {
  const set = new Uint8ArraySet();

  expect(set.size).toBe(0);

  const a1 = Uint8Array.from([1, 2, 3]);
  const a2 = Uint8Array.from([2, 3, 4]);

  expect(set.has(a1)).toBe(false);
  expect(set.has(a2)).toBe(false);
  expect(set.delete(a1)).toBe(false);
  expect(set.delete(a2)).toBe(false);
  expect(set.size).toBe(0);

  expect(set.add(a1)).toBe(true);
  expect(set.add(a1)).toBe(false);
  expect(set.has(a1)).toBe(true);
  expect(set.has(a2)).toBe(false);
  expect(set.has(Uint8Array.from(a1))).toBe(true);
  expect(set.has(Uint8Array.from(a2))).toBe(false);
  expect(set.size).toBe(1);

  expect(set.add(a2)).toBe(true);
  expect(set.add(a2)).toBe(false);
  expect(set.add(a1)).toBe(false);
  expect(set.has(a1)).toBe(true);
  expect(set.has(a2)).toBe(true);
  expect(set.has(Uint8Array.from(a1))).toBe(true);
  expect(set.has(Uint8Array.from(a2))).toBe(true);
  expect(set.size).toBe(2);

  expect(set.delete(a2)).toBe(true);
  expect(set.delete(a2)).toBe(false);
  expect(set.has(a1)).toBe(true);
  expect(set.has(a2)).toBe(false);
  expect(set.has(Uint8Array.from(a1))).toBe(true);
  expect(set.has(Uint8Array.from(a2))).toBe(false);
  expect(set.size).toBe(1);

  expect(set.delete(Uint8Array.from(a1))).toBe(true);
  expect(set.delete(a1)).toBe(false);
  expect(set.has(a1)).toBe(false);
  expect(set.has(a2)).toBe(false);
  expect(set.has(Uint8Array.from(a1))).toBe(false);
  expect(set.has(Uint8Array.from(a2))).toBe(false);
  expect(set.size).toBe(0);
});
