import { describe, expect, test } from "vitest";
import { compareUint8Arrays } from "../uint8-array/compareUint8Arrays";
import { insertPageEntry } from "../page-entries/pageEntries";
import {
  allocateAndInitBtreeRootPage,
  checkBtreeIntegrity,
  countBtreeEntries,
  EntriesRange,
  findAllBtreeEntriesWithPrefix,
  findFirstBtreeEntry,
  findFirstBtreeEntryWithPrefix,
  findLastBtreeEntry,
  insertBtreeEntry,
  removeBtreeEntry,
  scanBtreeEntries,
  scanBtreeEntriesReverse,
} from "./btree";
import { PageProvider, PageProviderForWrite } from "./pageProvider";

function leafPage(entries: number[][], pageSize: number): Uint8Array {
  const pageArray = new Uint8Array(pageSize);
  pageArray[0] = 0b10001;
  const entriesPageArray = new Uint8Array(pageArray.buffer, 1);
  entries.forEach((entry) => {
    expect(insertPageEntry(entriesPageArray, Uint8Array.from(entry))).toBe(true);
  });
  return pageArray;
}

function innerPage(entries: number[][], childPageNumbers: number[], pageSize: number): Uint8Array {
  const pageArray = new Uint8Array(pageSize);
  pageArray[0] = 0b10010;
  const childPageNumbersCount = Math.floor(pageArray.length / 16);
  const entriesPageArray = new Uint8Array(pageArray.buffer, 1 + childPageNumbersCount * 4);
  entries.forEach((entry) => {
    expect(insertPageEntry(entriesPageArray, Uint8Array.from(entry))).toBe(true);
  });
  const childPageNumbersDataView = new DataView(pageArray.buffer, 1, childPageNumbersCount * 4);
  childPageNumbers.forEach((childPageNumber, index) => {
    childPageNumbersDataView.setUint32(index * 4, childPageNumber);
  });
  return pageArray;
}

function createPageProvider(pageArrays: Uint8Array[]): PageProvider {
  return (pageNumber) => {
    return pageArrays[pageNumber];
  };
}

interface PageProviderForWriteWithExtra extends PageProviderForWrite {
  clone(): PageProviderForWriteWithExtra;
  pages: Uint8Array[];
  releasedPageNumbers: Set<number>;
}

function createPageProviderForWrite(pageSize: number, ...initialPages: Uint8Array[]): PageProviderForWriteWithExtra {
  const pages: Uint8Array[] = [...initialPages];
  const releasedPageNumbers = new Set<number>();
  function getPage(pageNumber: number): Uint8Array {
    const result = pages[pageNumber];
    if (!result) {
      throw new Error("page does not exist: " + pageNumber);
    }
    if (releasedPageNumbers.has(pageNumber)) {
      throw new Error("page was released: " + pageNumber);
    }
    return result;
  }
  return {
    getPage,
    getPageForUpdate: getPage,
    allocateNewPage() {
      // for now don't reuse released pages
      pages.push(new Uint8Array(pageSize));
      return pages.length - 1;
    },
    releasePage(pageNumber: number) {
      // call get to ensure the page exists
      getPage(pageNumber);
      releasedPageNumbers.add(pageNumber);
    },
    clone() {
      const result = createPageProviderForWrite(pageSize);
      pages.forEach((page) => result.pages.push(Uint8Array.from(page)));
      releasedPageNumbers.forEach((pageNumber) => result.releasedPageNumbers.add(pageNumber));
      return result;
    },
    pages,
    releasedPageNumbers,
  };
}

function testScanResult(
  pageProvider: PageProvider,
  params: { forward?: boolean; startEntry?: ArrayLike<number>; abort?: boolean },
  expected: ArrayLike<number>[],
  rootPageNumber = 0
): void {
  const result: Uint8Array[] = [];
  const callback = (entry: Uint8Array) => {
    result.push(entry);
    return !params.abort;
  };
  const startEntry = params.startEntry ? Uint8Array.from(params.startEntry) : undefined;
  let scanResult = params.forward
    ? scanBtreeEntries(pageProvider, rootPageNumber, startEntry, callback)
    : scanBtreeEntriesReverse(pageProvider, rootPageNumber, startEntry, callback);
  expect(scanResult).toBe(true);
  const expectedResult = expected.map((array) => Uint8Array.from(array));
  expect(result).toEqual(expectedResult);

  checkBtreeIntegrity(pageProvider, rootPageNumber);
}

function xorShift32(x: number): number {
  /* Algorithm "xor" from p. 4 of Marsaglia, "Xorshift RNGs" */
  x ^= x << 13;
  x ^= x >> 17;
  x ^= x << 5;
  return x;
}

function randomizeOrder<T>(array: T[], seed = 1): T[] {
  const positions = new Map<T, number>();
  let cur = seed;
  array.forEach((entry) => {
    cur = xorShift32(cur);
    positions.set(entry, cur);
  });
  return [...array].sort((a, b) => positions.get(a)! - positions.get(b)!);
}

describe("btree", () => {
  test("new/empty btree", () => {
    const pageProvider = createPageProvider([leafPage([], 400)]);
    testScanResult(pageProvider, { forward: true }, []);
    testScanResult(pageProvider, { forward: false }, []);
  });

  test("empty array entry", () => {
    const pageProvider = createPageProvider([leafPage([[]], 400)]);
    testScanResult(pageProvider, { forward: true }, [[]]);
    testScanResult(pageProvider, { forward: false }, [[]]);
  });

  test("empty array entry with other entry", () => {
    const pageProvider = createPageProvider([leafPage([[], [42]], 400)]);
    testScanResult(pageProvider, { forward: true }, [[], [42]]);
    testScanResult(pageProvider, { forward: false }, [[42], []]);
  });

  function testScanForOneAndThreeEntries(pageProvider: PageProvider, rootPageNumber = 0) {
    testScanResult(pageProvider, { forward: true }, [[1], [3]], rootPageNumber);
    testScanResult(pageProvider, { forward: false }, [[3], [1]], rootPageNumber);

    testScanResult(pageProvider, { forward: true, startEntry: [1] }, [[1], [3]], rootPageNumber);
    testScanResult(pageProvider, { forward: false, startEntry: [1] }, [[1]], rootPageNumber);
    testScanResult(pageProvider, { forward: true, startEntry: [2] }, [[3]], rootPageNumber);
    testScanResult(pageProvider, { forward: false, startEntry: [2] }, [[1]], rootPageNumber);
    testScanResult(pageProvider, { forward: true, startEntry: [3] }, [[3]], rootPageNumber);
    testScanResult(pageProvider, { forward: false, startEntry: [3] }, [[3], [1]], rootPageNumber);

    testScanResult(pageProvider, { forward: true, startEntry: [0] }, [[1], [3]], rootPageNumber);
    testScanResult(pageProvider, { forward: false, startEntry: [4] }, [[3], [1]], rootPageNumber);
  }

  test("only root page with entries", () => {
    const pageProvider = createPageProvider([leafPage([[1], [3]], 400)]);
    testScanForOneAndThreeEntries(pageProvider);
  });

  test("root page with two child pages", () => {
    const possibleMiddles = [[1, 0], [1, 1], [2], [2, 1], [3]];
    for (const possibleMiddle of possibleMiddles) {
      const pageProvider = createPageProvider([
        innerPage([possibleMiddle], [1, 2], 400),
        leafPage([[1]], 400),
        leafPage([[3]], 400),
      ]);
      testScanForOneAndThreeEntries(pageProvider);
    }
  });

  test("inner pages with just one child", () => {
    // this should usually not happen, but is allowed
    const possibleMiddles = [[1, 0], [1, 1], [2], [2, 1], [3]];
    for (const possibleMiddle of possibleMiddles) {
      const pageProvider = createPageProvider([
        innerPage([possibleMiddle], [1, 2], 400),
        innerPage([], [3], 400),
        innerPage([], [4], 400),
        leafPage([[1]], 400),
        leafPage([[3]], 400),
      ]);
      testScanForOneAndThreeEntries(pageProvider);
    }
  });

  test("root page with just one child", () => {
    // this should not happen, but is allowed
    const pageProvider = createPageProvider([innerPage([], [1], 400), leafPage([[1], [3]], 400)]);
    testScanForOneAndThreeEntries(pageProvider);
  });

  test("allocate and init", () => {
    const pageProvider = createPageProviderForWrite(400);
    const rootPageNumber = allocateAndInitBtreeRootPage(pageProvider);
    testScanResult(pageProvider.getPage, { forward: true }, [], rootPageNumber);
    testScanResult(pageProvider.getPage, { forward: false }, [], rootPageNumber);
  });

  test("insert 1 and 3", () => {
    const pageProvider = createPageProviderForWrite(400);
    const rootPageNumber = allocateAndInitBtreeRootPage(pageProvider);

    expect(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([3]))).toBe(true);
    expect(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([3]))).toBe(false);
    expect(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([1]))).toBe(true);
    expect(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([1]))).toBe(false);
    expect(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([3]))).toBe(false);
    expect(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([1]))).toBe(false);

    testScanForOneAndThreeEntries(pageProvider.getPage, rootPageNumber);

    expect(pageProvider.pages.length).toBe(1);
    expect(pageProvider.releasedPageNumbers.size).toBe(0);
  });

  function makeEntry(int32: number, length: number): Uint8Array {
    const result = new Uint8Array(length);
    new DataView(result.buffer).setInt32(0, int32);
    return result;
  }

  function testInsertAndRemove(originalEntries: Uint8Array[]) {
    const sortedEntries = [...originalEntries].sort(compareUint8Arrays);
    // test insert and remove in different orders (all combinations)
    const orders = [sortedEntries, [...sortedEntries].reverse(), randomizeOrder(sortedEntries)];

    for (const insertOrder of orders) {
      const pageProvider = createPageProviderForWrite(400);
      const rootPageNumber = allocateAndInitBtreeRootPage(pageProvider);

      const entries: Uint8Array[] = [];
      for (const entry of insertOrder) {
        entries.push(entry);
        entries.sort(compareUint8Arrays);
        expect(insertBtreeEntry(pageProvider, rootPageNumber, entry)).toBe(true);
        expect(insertBtreeEntry(pageProvider, rootPageNumber, entry)).toBe(false);

        testScanResult(pageProvider.getPage, { forward: true }, entries, rootPageNumber);
        expect(countBtreeEntries(pageProvider.getPage, rootPageNumber)).toBe(entries.length);
      }

      // test partial scans
      for (let i = 0; i < entries.length; i++) {
        testScanResult(
          pageProvider.getPage,
          { forward: true, startEntry: entries[i] },
          entries.slice(i),
          rootPageNumber
        );
        testScanResult(
          pageProvider.getPage,
          { forward: false, startEntry: entries[i] },
          entries.slice(0, i + 1).reverse(),
          rootPageNumber
        );
      }

      expect(pageProvider.pages.length).toBeGreaterThan(1);
      expect(pageProvider.releasedPageNumbers.size).toBe(0);

      // check that the first two layers are inner pages
      const rootPage = pageProvider.getPage(rootPageNumber);
      expect(rootPage[0]).toBe(0b10010);
      const childPage = pageProvider.getPage(new DataView(rootPage.buffer, rootPage.byteOffset + 1).getUint32(0));
      expect(childPage[0]).toBe(0b10010);

      for (const removeOrder of orders) {
        const removePageProvider = pageProvider.clone();

        let remaining = removeOrder.length;
        // remove the entries
        removeOrder.forEach((entry) => {
          expect(removeBtreeEntry(removePageProvider, rootPageNumber, entry)).toBe(true);
          expect(removeBtreeEntry(removePageProvider, rootPageNumber, entry)).toBe(false);
          checkBtreeIntegrity(removePageProvider.getPage, rootPageNumber);
          expect(countBtreeEntries(removePageProvider.getPage, rootPageNumber)).toBe(--remaining);
        });

        testScanResult(removePageProvider.getPage, { forward: true }, [], rootPageNumber);

        expect(removePageProvider.pages.length - removePageProvider.releasedPageNumbers.size).toBe(1);
        expect(removePageProvider.releasedPageNumbers.size).toBeGreaterThan(0);
      }
    }
  }

  test("insert and remove of ascending entries of equal size", () => {
    const entries: Uint8Array[] = [];
    for (let i = 0; i < 250; i++) {
      const entry = makeEntry(i, 40);
      entries.push(entry);
    }

    testInsertAndRemove(entries);
  });

  test("insert and remove of random entries of varying sizes", () => {
    // generate "random" entries with "random" lengths
    const entries: Uint8Array[] = [];
    let cur = 1;
    for (let i = 0; i < 200; i++) {
      const length = 4 + (Math.abs(cur) % 80);
      entries.push(makeEntry(cur, length));
      cur = xorShift32(cur);
    }

    testInsertAndRemove(entries);
  });

  test("leaf page merge during remove", () => {
    const entries = [[1], [2], [3], [4], [5], [6]];
    const middle1 = 2;
    const middle2 = 4;
    const pageProvider = createPageProviderForWrite(
      400,
      innerPage([entries[middle1], entries[middle2]], [1, 2, 3], 400),
      leafPage(entries.slice(0, middle1), 400),
      leafPage(entries.slice(middle1, middle2), 400),
      leafPage(entries.slice(middle2), 400)
    );
    const rootPageNumber = 0;

    testScanResult(pageProvider.getPage, { forward: true }, entries, rootPageNumber);

    entries.forEach((entry, index) => {
      const removePageProvider = pageProvider.clone();
      expect(removeBtreeEntry(removePageProvider, rootPageNumber, Uint8Array.from(entry))).toBe(true);
      expect(removeBtreeEntry(removePageProvider, rootPageNumber, Uint8Array.from(entry))).toBe(false);
      testScanResult(
        removePageProvider.getPage,
        { forward: true },
        entries.filter((e) => e !== entry),
        rootPageNumber
      );

      expect(removePageProvider.pages.length).toBe(4);
      if (index < middle1) {
        // no merge for the leftmost child
        expect(removePageProvider.releasedPageNumbers.size).toBe(0);
      } else {
        // removal of the entry should have merged two leaf pages
        expect(removePageProvider.releasedPageNumbers.size).toBe(1);
      }
    });
  });

  interface TestEntriesRange {
    start?: ArrayLike<number>;
    /** Defaults to false. */
    startExclusive?: boolean;

    end?: ArrayLike<number>;
    /** Defaults to true. */
    endExclusive?: boolean;
  }

  function testCount(
    pageProvider: PageProvider,
    rootPageNumber: number,
    expectedCount: number | undefined,
    testRange?: TestEntriesRange
  ) {
    const range: EntriesRange | undefined = testRange && {
      ...testRange,
      start: testRange.start && Uint8Array.from(testRange.start),
      end: testRange.end && Uint8Array.from(testRange.end),
    };
    expect(countBtreeEntries(pageProvider, rootPageNumber, range)).toBe(expectedCount);

    // also test the defaults for exclusive
    if (range?.startExclusive === undefined) {
      const rangeWithExplicit: EntriesRange = {
        ...(range || {}),
        startExclusive: false,
      };
      expect(countBtreeEntries(pageProvider, rootPageNumber, rangeWithExplicit)).toBe(expectedCount);
      if (rangeWithExplicit.endExclusive === undefined) {
        rangeWithExplicit.endExclusive = true;
        expect(countBtreeEntries(pageProvider, rootPageNumber, rangeWithExplicit)).toBe(expectedCount);
      }
    }
    if (range?.endExclusive === undefined) {
      const rangeWithExplicit: EntriesRange = {
        ...(range || {}),
        endExclusive: true,
      };
      expect(countBtreeEntries(pageProvider, rootPageNumber, rangeWithExplicit)).toBe(expectedCount);
    }
  }

  test("count with fixed tree", () => {
    const entries = [[1], [2], [3], [4], [5], [6], [6, 1], [7], [8]];
    const middle1 = 2;
    const middle2 = 4;
    const pageProvider = createPageProvider([
      innerPage([entries[middle1], [4, 1]], [1, 2, 3], 400),
      leafPage(entries.slice(0, middle1), 400),
      leafPage(entries.slice(middle1, middle2), 400),
      leafPage(entries.slice(middle2), 400),
    ]);
    const rootPageNumber = 0;

    testCount(pageProvider, rootPageNumber, entries.length);
    testCount(pageProvider, rootPageNumber, entries.length, { start: [0] });
    testCount(pageProvider, rootPageNumber, entries.length, { end: [100] });
    testCount(pageProvider, rootPageNumber, entries.length, { start: [0], end: [100] });

    testCount(pageProvider, rootPageNumber, 0, { start: [100], end: [0] });
    testCount(pageProvider, rootPageNumber, 0, { start: [100], end: [0], endExclusive: false });
    testCount(pageProvider, rootPageNumber, 0, { start: [100] });
    testCount(pageProvider, rootPageNumber, 0, { start: [100], startExclusive: true });
    testCount(pageProvider, rootPageNumber, 0, { end: [0] });
    testCount(pageProvider, rootPageNumber, 0, { end: [0], endExclusive: false });

    testCount(pageProvider, 100, undefined);

    for (let i = 0; i < entries.length; i++) {
      testCount(pageProvider, rootPageNumber, entries.length - i, {
        start: entries[i],
      });
      testCount(pageProvider, rootPageNumber, entries.length - i, {
        start: entries[i],
        end: [100],
      });
      testCount(pageProvider, rootPageNumber, Math.max(entries.length - i - 1, 0), {
        start: entries[i],
        startExclusive: true,
      });

      testCount(pageProvider, rootPageNumber, i, {
        end: entries[i],
      });
      testCount(pageProvider, rootPageNumber, i, {
        start: [0],
        end: entries[i],
      });
      testCount(pageProvider, rootPageNumber, Math.min(i + 1, entries.length), {
        end: entries[i],
        endExclusive: false,
      });

      for (let j = i; j < entries.length; j++) {
        testCount(pageProvider, rootPageNumber, j - i, {
          start: entries[i],
          end: entries[j],
        });
        testCount(pageProvider, rootPageNumber, j - i + 1, {
          start: entries[i],
          end: [...entries[j], 0],
        });
        testCount(pageProvider, rootPageNumber, j - i + 1, {
          start: entries[i],
          end: entries[j],
          endExclusive: false,
        });
        testCount(pageProvider, rootPageNumber, j - i, {
          start: entries[i],
          startExclusive: true,
          end: entries[j],
          endExclusive: false,
        });
      }
    }
  });

  test("find functions", () => {
    const pageProvider = createPageProviderForWrite(400);
    const rootPageNumber = allocateAndInitBtreeRootPage(pageProvider);

    expect(findFirstBtreeEntry(pageProvider.getPage, rootPageNumber)).toBe(undefined);
    expect(findLastBtreeEntry(pageProvider.getPage, rootPageNumber)).toBe(undefined);
    expect(findFirstBtreeEntryWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([1]))).toBe(undefined);
    expect(findAllBtreeEntriesWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([1]))).toStrictEqual([]);

    const entries: Uint8Array[] = [];
    const countPerGroup = 100;
    for (let i = 0; i < 3; i++) {
      for (let j = 0; j < countPerGroup; j++) {
        entries.push(Uint8Array.from([i, j, 1, 2, 3, 4, 5, 6, 7, 8]));
      }
    }
    for (const entry of entries) {
      expect(insertBtreeEntry(pageProvider, rootPageNumber, entry)).toBe(true);
    }

    expect(pageProvider.pages.length).toBeGreaterThan(1);

    expect(findFirstBtreeEntry(pageProvider.getPage, rootPageNumber)).toStrictEqual(entries[0]);
    expect(findLastBtreeEntry(pageProvider.getPage, rootPageNumber)).toStrictEqual(entries.at(-1));

    const group1 = entries.filter((e) => e[0] === 1);
    expect(findFirstBtreeEntryWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([1]))).toStrictEqual(
      group1[0]
    );
    expect(findAllBtreeEntriesWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([1]))).toStrictEqual(
      group1
    );

    expect(findFirstBtreeEntryWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([3]))).toBe(undefined);
    expect(findAllBtreeEntriesWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([3]))).toStrictEqual([]);

    const pagesMissingPageProvider: PageProvider = (pageNumber) => undefined;
    expect(findFirstBtreeEntry(pagesMissingPageProvider, rootPageNumber)).toBe(false);
    expect(findLastBtreeEntry(pagesMissingPageProvider, rootPageNumber)).toBe(false);

    expect(findFirstBtreeEntryWithPrefix(pagesMissingPageProvider, rootPageNumber, Uint8Array.from([3]))).toBe(false);
    expect(findAllBtreeEntriesWithPrefix(pagesMissingPageProvider, rootPageNumber, Uint8Array.from([3]))).toBe(false);
  });
});
