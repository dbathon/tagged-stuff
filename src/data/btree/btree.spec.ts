import { describe, expect, test } from "vitest";
import { insertPageEntry } from "../page-entries/pageEntries";
import { scanBtreeEntries, scanBtreeEntriesReverse } from "./btree";
import { PageProvider } from "./pageProvider";

function buildBtreePages(pageEntriesPerPage: number[][][], pageSize: number): PageProvider {
  const pageArrays = pageEntriesPerPage.map((entries) => {
    const pageArray = new Uint8Array(pageSize);
    entries.forEach((entry) => {
      expect(insertPageEntry(pageArray, Uint8Array.from(entry))).toBe(true);
    });
    return pageArray;
  });

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
    const pageProvider = buildBtreePages([[]], 400);
    testScanResult(pageProvider, { forward: true }, []);
    testScanResult(pageProvider, { forward: false }, []);
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
    const pageProvider = buildBtreePages([[[], [1], [3]]], 400);
    testScanForOneAndThreeEntries(pageProvider);
  });

  test("root page with two child pages", () => {
    const possibleMiddles = [[1, 0], [1, 1], [2], [2, 1], [3]];
    for (const possibleMiddle of possibleMiddles) {
      const pageProvider = buildBtreePages(
        [
          [
            [0, ...possibleMiddle],
            [1, 1, 0, 0, 0, 1],
            [1, 2, 0, 0, 0, 2],
          ],
          [[], [1]],
          [[], [3]],
        ],
        400
      );
      testScanForOneAndThreeEntries(pageProvider);
    }
  });
});
