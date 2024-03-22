import { describe, expect, test } from "vitest";
import { Patch } from "./Patch";

describe("createPatches", () => {
  const allZeroes = new Uint8Array(10);

  function checkPatches(base: Uint8Array, expected: Uint8Array, patches: Patch[]) {
    const copy = Uint8Array.from(base);
    patches.forEach((patch) => patch.applyTo(copy));
    expect(Array.from(copy)).toEqual(Array.from(expected));
  }

  test("trivial case", () => {
    const data = Uint8Array.from([0, 1, 0, 0, 0, 0, 0, 0, 0, 0]);
    const patches = Patch.createPatches(allZeroes, data, 10);
    expect(patches.length).toBe(1);
    const [patch] = patches;
    expect(patch.offset).toBe(1);
    expect(patch.bytes.length).toBe(1);
    checkPatches(allZeroes, data, patches);
  });

  test("two identical bytes inside the patch (serialized form has the shorter length than two patches", () => {
    const data = Uint8Array.from([0, 1, 0, 0, 2, 0, 0, 0, 0, 0]);
    const patches = Patch.createPatches(allZeroes, data, 10);
    expect(patches.length).toBe(1);
    const [patch] = patches;
    expect(patch.offset).toBe(1);
    expect(patch.bytes.length).toBe(4);
    checkPatches(allZeroes, data, patches);
  });

  test("three identical bytes inside the patch (serialized form has the same length as two patches", () => {
    const data = Uint8Array.from([0, 1, 0, 0, 0, 2, 0, 0, 0, 0]);
    const patches = Patch.createPatches(allZeroes, data, 10);
    expect(patches.length).toBe(1);
    const [patch] = patches;
    expect(patch.offset).toBe(1);
    expect(patch.bytes.length).toBe(5);
    checkPatches(allZeroes, data, patches);
  });

  test("four identical bytes between two patches", () => {
    const data = Uint8Array.from([0, 1, 0, 0, 0, 0, 2, 0, 0, 0]);
    const patches = Patch.createPatches(allZeroes, data, 10);
    expect(patches.length).toBe(2);
    const [patch1, patch2] = patches;
    expect(patch1.offset).toBe(1);
    expect(patch1.bytes.length).toBe(1);
    expect(patch2.offset).toBe(6);
    expect(patch2.bytes.length).toBe(1);
    checkPatches(allZeroes, data, patches);
  });

  test("long difference results in two patches", () => {
    const zeros = new Uint8Array(300);
    const mostlyOnes = new Uint8Array(300);
    for (let i = 1; i < 299; i++) {
      mostlyOnes[i] = 1;
    }
    const patches = Patch.createPatches(zeros, mostlyOnes, 300);
    expect(patches.length).toBe(2);
    const [patch1, patch2] = patches;
    expect(patch1.offset).toBe(1);
    expect(patch1.bytes.length).toBe(255);
    expect(patch2.offset).toBe(256);
    expect(patch2.bytes.length).toBe(43);
    checkPatches(zeros, mostlyOnes, patches);
  });
});
