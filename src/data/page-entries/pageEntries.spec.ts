import { describe, expect, test } from "vitest";
import { insertPageEntry, readAllPageEntries } from "./pageEntries";

describe("pageEntries", () => {
  test("blank page has zero entries", () => {
    const pageArray = new Uint8Array(4000);
    const entries = readAllPageEntries(pageArray);
    expect(entries.length).toBe(0);
  });

  test("insert works", () => {
    const testEntries = [[], [1], [5, 45, 5], [12], [12, 23], [20]].map((array) => Uint8Array.from(array));

    const insertOrders = [
      [0, 1, 2, 3, 4, 5],
      [5, 1, 2, 3, 4, 0],
      [3, 4, 5, 0, 1, 2],
      [3, 4, 0, 1, 2, 5],
      [5, 4, 3, 2, 1, 0],
    ];

    for (const order of insertOrders) {
      const pageArray = new Uint8Array(4000);
      for (const index of order) {
        expect(insertPageEntry(pageArray, testEntries[index])).toBe(true);
      }

      const entries = readAllPageEntries(pageArray);
      expect(entries).toEqual(testEntries);
    }
  });
});
