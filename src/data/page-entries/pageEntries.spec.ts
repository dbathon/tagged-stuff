import { describe, expect, test } from "vitest";
import { insertPageEntry, readAllPageEntries, readPageEntriesFreeSpace, removePageEntry } from "./pageEntries";

// some copied constants
const FREE_CHUNKS_POINTER = 3;

describe("pageEntries", () => {
  test("blank page has zero entries", () => {
    const pageArray = new Uint8Array(100);
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
      // also test inserting entries multiple times
      [0, 1, 1, 1, 2, 3, 1, 0, 4, 5],
      [5, 1, 2, 3, 4, 0, 5, 1, 2, 3, 4, 0],
      [3, 4, 5, 0, 1, 2, 5, 4, 3, 2, 1, 0],
    ];

    for (const order of insertOrders) {
      const pageArray = new Uint8Array(100);
      const dataView = new DataView(pageArray.buffer);
      for (const index of order) {
        expect(insertPageEntry(pageArray, testEntries[index])).toBe(true);
      }

      const entries = readAllPageEntries(pageArray);
      expect(entries).toEqual(testEntries);

      // no free chunks exist
      expect(dataView.getUint16(FREE_CHUNKS_POINTER)).toBe(0);
    }
  });

  test("remove works", () => {
    const pageArray = new Uint8Array(400);
    const dataView = new DataView(pageArray.buffer);

    const startFreeSpace = readPageEntriesFreeSpace(pageArray);
    expect(startFreeSpace).toBeGreaterThan(0);

    const entries: Uint8Array[] = [];
    const count = 20;
    for (let i = 0; i < count; i++) {
      const entry = Uint8Array.from([i, 0, 1, 2, 3, 4, 5]);
      expect(insertPageEntry(pageArray, entry)).toBe(true);
      entries.push(entry);
      expect(readPageEntriesFreeSpace(pageArray)).toBeLessThan(startFreeSpace);
    }

    expect(readAllPageEntries(pageArray)).toEqual(entries);
    // no free chunks exist
    expect(dataView.getUint16(FREE_CHUNKS_POINTER)).toBe(0);

    // remove the first entry
    expect(removePageEntry(pageArray, entries[0])).toBe(true);
    // removing again should return false
    expect(removePageEntry(pageArray, entries[0])).toBe(false);

    expect(readAllPageEntries(pageArray)).toEqual(entries.slice(1));
    // free chunks exists
    expect(dataView.getUint16(FREE_CHUNKS_POINTER)).not.toBe(0);

    // remove the other entries
    for (let i = 1; i < count; i++) {
      expect(removePageEntry(pageArray, entries[i])).toBe(true);
      // removing again should return false
      expect(removePageEntry(pageArray, entries[i])).toBe(false);
    }

    expect(readAllPageEntries(pageArray)).toEqual([]);
    // no free chunks exist
    expect(dataView.getUint16(FREE_CHUNKS_POINTER)).toBe(0);
    expect(readPageEntriesFreeSpace(pageArray)).toBe(startFreeSpace);
  });
});
