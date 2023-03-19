import { describe, expect, test } from "vitest";
import { compareUint8Arrays } from "../page-entries/compareUint8Arrays";
import { insertPageEntry } from "../page-entries/pageEntries";
import { allocateAndInitBtreeRootPage, insertBtreeEntry, scanBtreeEntries, scanBtreeEntriesReverse } from "./btree";
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
    const result = pageArrays[pageNumber];
    if (!result) {
      throw new Error("unknown page: " + pageNumber);
    }
    return result;
  };
}

interface PageProviderForWriteWithExtra extends PageProviderForWrite {
  pages: Uint8Array[];
  releasedPageNumbers: Set<number>;
}

function createPageProviderForWrite(pageSize: number): PageProviderForWriteWithExtra {
  const pages: Uint8Array[] = [];
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
  expect(expectedResult).toEqual(result);
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

  function xorShift32(x: number): number {
    /* Algorithm "xor" from p. 4 of Marsaglia, "Xorshift RNGs" */
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    return x;
  }

  test("insert larger entries", () => {
    const pageProvider = createPageProviderForWrite(400);
    const rootPageNumber = allocateAndInitBtreeRootPage(pageProvider);

    const entries: Uint8Array[] = [];

    // do inserts in a "random" order
    let cur = 1;
    for (let i = 0; i < 150; i++) {
      const newEntry = makeEntry(cur, 64);
      entries.push(newEntry);
      entries.sort(compareUint8Arrays);
      expect(insertBtreeEntry(pageProvider, rootPageNumber, newEntry)).toBe(true);
      expect(insertBtreeEntry(pageProvider, rootPageNumber, newEntry)).toBe(false);

      testScanResult(pageProvider.getPage, { forward: true }, entries, rootPageNumber);
      cur = xorShift32(cur);
    }

    // test partial scans
    for (let i = 0; i < entries.length; i++) {
      testScanResult(pageProvider.getPage, { forward: true, startEntry: entries[i] }, entries.slice(i), rootPageNumber);
      testScanResult(
        pageProvider.getPage,
        { forward: false, startEntry: entries[i] },
        entries.slice(0, i + 1).reverse(),
        rootPageNumber
      );
    }

    expect(pageProvider.pages.length).toBeGreaterThan(1);
    expect(pageProvider.releasedPageNumbers.size).toBe(0);

    // check that the first too layers are inner pages
    const rootPage = pageProvider.getPage(rootPageNumber);
    expect(rootPage[0]).toBe(0b10010);
    const childPage = pageProvider.getPage(new DataView(rootPage.buffer, rootPage.byteOffset + 1).getUint32(0));
    expect(childPage[0]).toBe(0b10010);
  });
});
