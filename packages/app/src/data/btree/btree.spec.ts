import { describe, assert, expect, test } from "vitest";
import { compareUint8Arrays, uint8ArraysEqual } from "shared-util";
import { insertPageEntry } from "../page-entries/pageEntries";
import {
  allocateAndInitBtreeRootPage,
  checkBtreeIntegrity,
  countBtreeEntries,
  type EntriesRange,
  findAllBtreeEntriesWithPrefix,
  findFirstBtreeEntry,
  findFirstBtreeEntryWithPrefix,
  findLastBtreeEntry,
  insertBtreeEntry,
  removeBtreeEntry,
  scanBtreeEntries,
  scanBtreeEntriesReverse,
} from "./btree";
import { type PageProvider, type PageProviderForWrite } from "./pageProvider";

function leafPage(entries: number[][], pageSize: number): Uint8Array {
  const pageArray = new Uint8Array(pageSize);
  pageArray[0] = 0b10001;
  const entriesPageArray = new Uint8Array(pageArray.buffer, 1);
  entries.forEach((entry) => {
    assert(insertPageEntry(entriesPageArray, Uint8Array.from(entry)) === true);
  });
  return pageArray;
}

function innerPage(entries: number[][], childPageNumbers: number[], pageHeight: number, pageSize: number): Uint8Array {
  const pageArray = new Uint8Array(pageSize);
  pageArray[0] = 0b10010;
  pageArray[1] = pageHeight;
  const childPageNumbersCount = Math.floor(pageArray.length / 16);
  const entriesPageArray = new Uint8Array(pageArray.buffer, 2 + childPageNumbersCount * 4);
  entries.forEach((entry) => {
    assert(insertPageEntry(entriesPageArray, Uint8Array.from(entry)) === true);
  });
  const childPageNumbersDataView = new DataView(pageArray.buffer, 2, childPageNumbersCount * 4);
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
  assert(scanResult === true);
  assert(result.length === expected.length);
  for (let i = 0; i < result.length; i++) {
    assert(uint8ArraysEqual(result[i], Uint8Array.from(expected[i])));
  }

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
        innerPage([possibleMiddle], [1, 2], 1, 400),
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
        innerPage([possibleMiddle], [1, 2], 2, 400),
        innerPage([], [3], 1, 400),
        innerPage([], [4], 1, 400),
        leafPage([[1]], 400),
        leafPage([[3]], 400),
      ]);
      testScanForOneAndThreeEntries(pageProvider);
    }
  });

  test("root page with just one child", () => {
    // this should not happen, but is allowed
    const pageProvider = createPageProvider([innerPage([], [1], 1, 400), leafPage([[1], [3]], 400)]);
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

    assert(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([3])) === true);
    assert(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([3])) === false);
    assert(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([1])) === true);
    assert(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([1])) === false);
    assert(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([3])) === false);
    assert(insertBtreeEntry(pageProvider, rootPageNumber, Uint8Array.from([1])) === false);

    testScanForOneAndThreeEntries(pageProvider.getPage, rootPageNumber);

    assert(pageProvider.pages.length === 1);
    assert(pageProvider.releasedPageNumbers.size === 0);
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
        assert(insertBtreeEntry(pageProvider, rootPageNumber, entry) === true);
        assert(insertBtreeEntry(pageProvider, rootPageNumber, entry) === false);

        testScanResult(pageProvider.getPage, { forward: true }, entries, rootPageNumber);
        assert(countBtreeEntries(pageProvider.getPage, rootPageNumber) === entries.length);
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

      assert(pageProvider.pages.length > 1);
      assert(pageProvider.releasedPageNumbers.size === 0);

      // check that the first two layers are inner pages
      const rootPage = pageProvider.getPage(rootPageNumber);
      assert(rootPage[0] === 0b10010);
      const childPage = pageProvider.getPage(new DataView(rootPage.buffer, rootPage.byteOffset + 2).getUint32(0));
      assert(childPage[0] === 0b10010);

      for (const removeOrder of orders) {
        const removePageProvider = pageProvider.clone();

        let remaining = removeOrder.length;
        // remove the entries
        removeOrder.forEach((entry) => {
          assert(removeBtreeEntry(removePageProvider, rootPageNumber, entry) === true);
          assert(removeBtreeEntry(removePageProvider, rootPageNumber, entry) === false);
          checkBtreeIntegrity(removePageProvider.getPage, rootPageNumber);
          assert(countBtreeEntries(removePageProvider.getPage, rootPageNumber) === --remaining);
        });

        testScanResult(removePageProvider.getPage, { forward: true }, [], rootPageNumber);

        assert(removePageProvider.pages.length - removePageProvider.releasedPageNumbers.size === 1);
        assert(removePageProvider.releasedPageNumbers.size > 0);
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
      innerPage([entries[middle1], entries[middle2]], [1, 2, 3], 1, 400),
      leafPage(entries.slice(0, middle1), 400),
      leafPage(entries.slice(middle1, middle2), 400),
      leafPage(entries.slice(middle2), 400)
    );
    const rootPageNumber = 0;

    testScanResult(pageProvider.getPage, { forward: true }, entries, rootPageNumber);

    entries.forEach((entry, index) => {
      const removePageProvider = pageProvider.clone();
      assert(removeBtreeEntry(removePageProvider, rootPageNumber, Uint8Array.from(entry)) === true);
      assert(removeBtreeEntry(removePageProvider, rootPageNumber, Uint8Array.from(entry)) === false);
      testScanResult(
        removePageProvider.getPage,
        { forward: true },
        entries.filter((e) => e !== entry),
        rootPageNumber
      );

      assert(removePageProvider.pages.length === 4);
      if (index < middle1) {
        // no merge for the leftmost child
        assert(removePageProvider.releasedPageNumbers.size === 0);
      } else {
        // removal of the entry should have merged two leaf pages
        assert(removePageProvider.releasedPageNumbers.size === 1);
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
    assert(countBtreeEntries(pageProvider, rootPageNumber, range) === expectedCount);

    // also test the defaults for exclusive
    if (range?.startExclusive === undefined) {
      const rangeWithExplicit: EntriesRange = {
        ...(range || {}),
        startExclusive: false,
      };
      assert(countBtreeEntries(pageProvider, rootPageNumber, rangeWithExplicit) === expectedCount);
      if (rangeWithExplicit.endExclusive === undefined) {
        rangeWithExplicit.endExclusive = true;
        assert(countBtreeEntries(pageProvider, rootPageNumber, rangeWithExplicit) === expectedCount);
      }
    }
    if (range?.endExclusive === undefined) {
      const rangeWithExplicit: EntriesRange = {
        ...(range || {}),
        endExclusive: true,
      };
      assert(countBtreeEntries(pageProvider, rootPageNumber, rangeWithExplicit) === expectedCount);
    }
  }

  test("count with fixed tree", () => {
    const entries = [[1], [2], [3], [4], [5], [6], [6, 1], [7], [8]];
    const middle1 = 2;
    const middle2 = 4;
    const pageProvider = createPageProvider([
      innerPage([entries[middle1], [4, 1]], [1, 2, 3], 1, 400),
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

    assert(findFirstBtreeEntry(pageProvider.getPage, rootPageNumber) === undefined);
    assert(findLastBtreeEntry(pageProvider.getPage, rootPageNumber) === undefined);
    assert(findFirstBtreeEntryWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([1])) === undefined);
    expect(findAllBtreeEntriesWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([1]))).toStrictEqual([]);

    const entries: Uint8Array[] = [];
    const countPerGroup = 100;
    for (let i = 0; i < 3; i++) {
      for (let j = 0; j < countPerGroup; j++) {
        entries.push(Uint8Array.from([i, j, 1, 2, 3, 4, 5, 6, 7, 8]));
      }
    }
    for (const entry of entries) {
      assert(insertBtreeEntry(pageProvider, rootPageNumber, entry) === true);
    }

    assert(pageProvider.pages.length > 1);

    expect(findFirstBtreeEntry(pageProvider.getPage, rootPageNumber)).toStrictEqual(entries[0]);
    expect(findLastBtreeEntry(pageProvider.getPage, rootPageNumber)).toStrictEqual(entries.at(-1));

    const group1 = entries.filter((e) => e[0] === 1);
    expect(findFirstBtreeEntryWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([1]))).toStrictEqual(
      group1[0]
    );
    expect(findAllBtreeEntriesWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([1]))).toStrictEqual(
      group1
    );

    assert(findFirstBtreeEntryWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([3])) === undefined);
    expect(findAllBtreeEntriesWithPrefix(pageProvider.getPage, rootPageNumber, Uint8Array.from([3]))).toStrictEqual([]);

    const pagesMissingPageProvider: PageProvider = (pageNumber) => undefined;
    assert(findFirstBtreeEntry(pagesMissingPageProvider, rootPageNumber) === false);
    assert(findLastBtreeEntry(pagesMissingPageProvider, rootPageNumber) === false);

    assert(findFirstBtreeEntryWithPrefix(pagesMissingPageProvider, rootPageNumber, Uint8Array.from([3])) === false);
    assert(findAllBtreeEntriesWithPrefix(pagesMissingPageProvider, rootPageNumber, Uint8Array.from([3])) === false);
  });
});
