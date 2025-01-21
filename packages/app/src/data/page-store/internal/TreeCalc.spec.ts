import { assert, expect, test } from "vitest";
import { TreeCalc, type TransactionIdLocation } from "./TreeCalc";

class TestCase {
  constructor(
    readonly treeCalc: TreeCalc,
    readonly expectedHeight: number,
  ) {}

  verifyPath(path: TransactionIdLocation[]) {
    assert(path.length <= this.expectedHeight);

    let prevElement: TransactionIdLocation | undefined = undefined;
    for (const element of path) {
      assert(element.pageNumber > this.treeCalc.maxNormalPageNumber);
      assert(element.offset >= 0);
      assert(element.offset + this.treeCalc.entrySize <= this.treeCalc.pageSize);

      if (prevElement) {
        assert(element.pageNumber > prevElement.pageNumber);
      }
      prevElement = element;
    }
  }

  getPath(pageNumber: number): TransactionIdLocation[] {
    const location = this.treeCalc.getTransactionIdLocation(pageNumber);
    if (!location) {
      return [];
    }
    return [...this.getPath(location.pageNumber), location];
  }
}

test("TreeCalc", () => {
  const testCases = [
    new TestCase(new TreeCalc(1 << 10, 6, -1 >>> 0), 5),
    new TestCase(new TreeCalc(1 << 12, 6, -1 >>> 0), 4),
    new TestCase(new TreeCalc(1 << 13, 6, -1 >>> 0), 4),
    new TestCase(new TreeCalc(1 << 14, 6, -1 >>> 0), 3),
  ];

  for (const testCase of testCases) {
    const { treeCalc, expectedHeight } = testCase;
    assert(treeCalc.height === expectedHeight);

    for (const normalPath of [
      testCase.getPath(0),
      testCase.getPath(Math.floor(treeCalc.maxNormalPageNumber / 2)),
      testCase.getPath(treeCalc.maxNormalPageNumber),
    ]) {
      assert(normalPath.length === expectedHeight);
      testCase.verifyPath(normalPath);

      // verify that getPath() also works for the page numbers in the path
      const remainingPath = [...normalPath];
      while (remainingPath.length) {
        const lastElement = remainingPath.pop()!;
        assert(lastElement.pageNumber > treeCalc.maxNormalPageNumber);
        expect(testCase.getPath(lastElement.pageNumber)).toEqual(remainingPath);
      }
    }

    const rootPagePath = testCase.getPath(treeCalc.maxNormalPageNumber + 1);
    assert(rootPagePath.length === 0);
    testCase.verifyPath(rootPagePath);

    const lastPagePath = testCase.getPath(treeCalc.maxPageNumber);
    assert(lastPagePath.length === expectedHeight - 1);
    testCase.verifyPath(lastPagePath);
  }
});
