import { describe, expect, test } from "vitest";
import {
  insertPageEntry,
  readAllPageEntries,
  readPageEntriesFreeSpace,
  removePageEntry,
  scanPageEntries,
  scanPageEntriesReverse,
} from "./pageEntries";

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

  test("scanPageEntries", () => {
    const pageArray = new Uint8Array(100);

    const collected: [Uint8Array, number][] = [];
    let collectAll = true;
    function collectCallback(entry: Uint8Array, entryNumber: number): boolean {
      collected.push([entry, entryNumber]);
      return collectAll;
    }
    function getAndClearCollected(): [Uint8Array, number][] {
      const result = [...collected];
      collected.length = 0;
      return result;
    }

    function testEntry(byte: number) {
      return Uint8Array.from([byte]);
    }
    const testEntryPairs: [Uint8Array, number][] = [
      [testEntry(1), 0],
      [testEntry(3), 1],
      [testEntry(5), 2],
    ];
    testEntryPairs.forEach(([entry]) => {
      expect(insertPageEntry(pageArray, entry)).toBe(true);
    });

    const forwardCases: [Uint8Array | number | undefined, number][] = [
      [testEntry(0), 0],
      [testEntry(1), 0],
      [testEntry(2), 1],
      [testEntry(3), 1],
      [testEntry(4), 2],
      [testEntry(5), 2],
      [testEntry(6), 3],
      [testEntry(7), 3],
      [-1, 3],
      [0, 0],
      [1, 1],
      [2, 2],
      [3, 3],
      [4, 3],
      [undefined, 0],
    ];
    forwardCases.forEach(([startEntryOrEntryNumber, sliceStart]) => {
      const expected = testEntryPairs.slice(sliceStart);
      collectAll = true;
      scanPageEntries(pageArray, startEntryOrEntryNumber, collectCallback);
      expect(getAndClearCollected()).toEqual(expected);

      collectAll = false;
      scanPageEntries(pageArray, startEntryOrEntryNumber, collectCallback);
      expect(getAndClearCollected()).toEqual(expected.slice(0, 1));
    });

    const reverseCases: [Uint8Array | number | undefined, number][] = [
      [testEntry(0), 0],
      [testEntry(1), 1],
      [testEntry(2), 1],
      [testEntry(3), 2],
      [testEntry(4), 2],
      [testEntry(5), 3],
      [testEntry(6), 3],
      [testEntry(7), 3],
      [-1, 0],
      [0, 1],
      [1, 2],
      [2, 3],
      [3, 0],
      [4, 0],
      [undefined, 3],
    ];
    reverseCases.forEach(([startEntryOrEntryNumber, sliceCount]) => {
      const expected = testEntryPairs.slice(0, sliceCount).reverse();
      collectAll = true;
      scanPageEntriesReverse(pageArray, startEntryOrEntryNumber, collectCallback);
      expect(getAndClearCollected()).toEqual(expected);

      collectAll = false;
      scanPageEntriesReverse(pageArray, startEntryOrEntryNumber, collectCallback);
      expect(getAndClearCollected()).toEqual(expected.slice(0, 1));
    });
  });
});
