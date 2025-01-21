import { describe, assert, expect, test } from "vitest";
import {
  containsPageEntry,
  insertPageEntry,
  readAllPageEntries,
  readPageEntriesCount,
  readPageEntriesFreeSpace,
  removePageEntry,
  scanPageEntries,
  scanPageEntriesReverse,
} from "./pageEntries";

// some copied constants
const FREE_CHUNKS_SIZE = 3;

describe("pageEntries", () => {
  test("blank page has zero entries", () => {
    const pageArray = new Uint8Array(100);
    const entries = readAllPageEntries(pageArray);
    assert(entries.length === 0);
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
        assert(insertPageEntry(pageArray, testEntries[index]) === true);
      }

      const entries = readAllPageEntries(pageArray);
      expect(entries).toEqual(testEntries);

      // no free chunks exist
      assert(dataView.getUint16(FREE_CHUNKS_SIZE) === 0);
    }
  });

  test("remove works", () => {
    const pageArray = new Uint8Array(400);
    const dataView = new DataView(pageArray.buffer);

    const startFreeSpace = readPageEntriesFreeSpace(pageArray);
    assert(startFreeSpace > 0);

    const entries: Uint8Array[] = [];
    const count = 20;
    for (let i = 0; i < count; i++) {
      const entry = Uint8Array.from([i, 0, 1, 2, 3, 4, 5]);
      assert(insertPageEntry(pageArray, entry) === true);
      entries.push(entry);
      expect(readPageEntriesFreeSpace(pageArray)).toBeLessThan(startFreeSpace);
    }

    expect(readAllPageEntries(pageArray)).toEqual(entries);
    // no free chunks exist
    assert(dataView.getUint16(FREE_CHUNKS_SIZE) === 0);

    // remove the first entry
    assert(removePageEntry(pageArray, entries[0]) === true);
    // removing again should return false
    assert(removePageEntry(pageArray, entries[0]) === false);

    expect(readAllPageEntries(pageArray)).toEqual(entries.slice(1));
    // free chunks exists
    expect(dataView.getUint16(FREE_CHUNKS_SIZE)).not.toBe(0);

    // remove the other entries
    for (let i = 1; i < count; i++) {
      assert(removePageEntry(pageArray, entries[i]) === true);
      // removing again should return false
      assert(removePageEntry(pageArray, entries[i]) === false);
    }

    expect(readAllPageEntries(pageArray)).toEqual([]);
    // no free chunks exist
    assert(dataView.getUint16(FREE_CHUNKS_SIZE) === 0);
    assert(readPageEntriesFreeSpace(pageArray) === startFreeSpace);
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
      assert(insertPageEntry(pageArray, entry) === true);
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

  test("max entry size", () => {
    const pageArray = new Uint8Array(0xffff);

    const maxSizeEntry = new Uint8Array(2000);
    assert(insertPageEntry(pageArray, maxSizeEntry) === true);

    const tooLargeEntry = new Uint8Array(2001);
    expect(() => insertPageEntry(pageArray, tooLargeEntry)).toThrow();
  });

  function xorShift32(x: number): number {
    /* Algorithm "xor" from p. 4 of Marsaglia, "Xorshift RNGs" */
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    return x >>> 0;
  }

  function newRandom(seed: number): () => number {
    let state = seed;
    return () => {
      const result = state;
      state = xorShift32(state);
      return result;
    };
  }

  test("random inserts and removes", () => {
    const pageArray = new Uint8Array(0xffff);
    const dataView = new DataView(pageArray.buffer);

    const random = newRandom(1);
    const entries: Uint8Array[] = [];

    const freeSpaceAtStart = readPageEntriesFreeSpace(pageArray);

    // add random entries
    while (true) {
      const entry = new Uint8Array(random() % 2001);
      for (let i = 0; i < entry.length; i++) {
        entry[i] = random() % 256;
      }

      assert(containsPageEntry(pageArray, entry) === false);

      const insertSuccess = insertPageEntry(pageArray, entry);
      if (!insertSuccess) {
        // pageArray is "full"
        break;
      }

      entries.push(entry);
    }

    assert(entries.length > 0);
    assert(readPageEntriesCount(pageArray) === entries.length);
    assert(dataView.getUint16(FREE_CHUNKS_SIZE) === 0);

    entries.forEach((entry) => {
      assert(containsPageEntry(pageArray, entry) === true);
    });

    // remove each one and immediately insert it again and check that the free chunk is reused
    entries.forEach((entry) => {
      const freeSpaceBefore = readPageEntriesFreeSpace(pageArray);
      assert(removePageEntry(pageArray, entry) === true);
      assert(removePageEntry(pageArray, entry) === false);
      assert(readPageEntriesFreeSpace(pageArray) > freeSpaceBefore);

      assert(insertPageEntry(pageArray, entry) === true);
      assert(readPageEntriesFreeSpace(pageArray) === freeSpaceBefore);
      assert(dataView.getUint16(FREE_CHUNKS_SIZE) === 0);
    });

    // remove half of them
    const removed: Uint8Array[] = [];
    while (removed.length < entries.length) {
      const index = random() % entries.length;
      const entry = entries[index];
      assert(removePageEntry(pageArray, entry) === true);
      entries.splice(index, 1);
      removed.push(entry);
    }

    assert(readPageEntriesCount(pageArray) === entries.length);
    assert(dataView.getUint16(FREE_CHUNKS_SIZE) > 0);

    entries.forEach((entry) => {
      assert(containsPageEntry(pageArray, entry) === true);
    });

    removed.forEach((entry) => {
      assert(containsPageEntry(pageArray, entry) === false);
    });

    // try re-adding the entries (not all might work because of fragmentation)
    let reinsertCount = 0;
    while (removed.length) {
      const index = random() % removed.length;
      const entry = removed[index];
      assert(containsPageEntry(pageArray, entry) === false);
      if (insertPageEntry(pageArray, entry)) {
        removed.splice(index, 1);
        entries.push(entry);
        ++reinsertCount;
      } else {
        break;
      }
    }
    assert(reinsertCount > 0);

    entries.forEach((entry) => {
      assert(containsPageEntry(pageArray, entry) === true);
    });

    removed.forEach((entry) => {
      assert(containsPageEntry(pageArray, entry) === false);
    });

    // remove all entries again
    entries.forEach((entry) => {
      assert(removePageEntry(pageArray, entry) === true);
    });

    assert(readPageEntriesCount(pageArray) === 0);
    assert(readPageEntriesFreeSpace(pageArray) === freeSpaceAtStart);
    assert(dataView.getUint16(FREE_CHUNKS_SIZE) === 0);
  });
});
