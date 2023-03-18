import { describe, expect, test } from "vitest";
import { insertPageEntry } from "../page-entries/pageEntries";
import { scanBtreeEntries, scanBtreeEntriesReverse } from "./btree";
import { PageProvider } from "./pageProvider";

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

function testScanResult(
  pageProvider: PageProvider,
  params: { forward?: boolean; startEntry?: number[]; abort?: boolean },
  expected: number[][]
): void {
  const result: Uint8Array[] = [];
  const callback = (entry: Uint8Array) => {
    result.push(entry);
    return !params.abort;
  };
  const startEntry = params.startEntry ? Uint8Array.from(params.startEntry) : undefined;
  let scanResult = params.forward
    ? scanBtreeEntries(pageProvider, 0, startEntry, callback)
    : scanBtreeEntriesReverse(pageProvider, 0, startEntry, callback);
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

  function testScanForOneAndThreeEntries(pageProvider: PageProvider) {
    testScanResult(pageProvider, { forward: true }, [[1], [3]]);
    testScanResult(pageProvider, { forward: false }, [[3], [1]]);

    testScanResult(pageProvider, { forward: true, startEntry: [1] }, [[1], [3]]);
    testScanResult(pageProvider, { forward: false, startEntry: [1] }, [[1]]);
    testScanResult(pageProvider, { forward: true, startEntry: [2] }, [[3]]);
    testScanResult(pageProvider, { forward: false, startEntry: [2] }, [[1]]);
    testScanResult(pageProvider, { forward: true, startEntry: [3] }, [[3]]);
    testScanResult(pageProvider, { forward: false, startEntry: [3] }, [[3], [1]]);

    testScanResult(pageProvider, { forward: true, startEntry: [0] }, [[1], [3]]);
    testScanResult(pageProvider, { forward: false, startEntry: [4] }, [[3], [1]]);
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
});
