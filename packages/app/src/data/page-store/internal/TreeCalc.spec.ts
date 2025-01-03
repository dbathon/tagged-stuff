import { expect, test } from "vitest";
import { TreeCalc, type TreePathElement } from "./TreeCalc";

class TestCase {
  constructor(readonly treeCalc: TreeCalc, readonly expectedHeight: number) {}

  verifyPath(path: TreePathElement[]) {
    expect(path.length).toBeLessThanOrEqual(this.expectedHeight);

    let prevElement: TreePathElement | undefined = undefined;
    for (const element of path) {
      expect(element.pageNumber).toBeGreaterThan(this.treeCalc.maxNormalPageNumber);
      expect(element.offset).toBeGreaterThanOrEqual(0);
      expect(element.offset + this.treeCalc.entrySize).toBeLessThanOrEqual(this.treeCalc.pageSize);

      if (prevElement) {
        expect(element.pageNumber).toBeGreaterThan(prevElement.pageNumber);
      }
      prevElement = element;
    }
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
    expect(treeCalc.height).toBe(expectedHeight);

    for (const normalPath of [
      treeCalc.getPath(0),
      treeCalc.getPath(Math.floor(treeCalc.maxNormalPageNumber / 2)),
      treeCalc.getPath(treeCalc.maxNormalPageNumber),
    ]) {
      expect(normalPath.length).toBe(expectedHeight);
      testCase.verifyPath(normalPath);

      // verify that getPath() also works for the page numbers in the path
      const remainingPath = [...normalPath];
      while (remainingPath.length) {
        const lastElement = remainingPath.pop()!;
        expect(lastElement.pageNumber).toBeGreaterThan(treeCalc.maxNormalPageNumber);
        expect(treeCalc.getPath(lastElement.pageNumber)).toEqual(remainingPath);
      }
    }

    const rootPagePath = treeCalc.getPath(treeCalc.maxNormalPageNumber + 1);
    expect(rootPagePath.length).toBe(0);
    testCase.verifyPath(rootPagePath);

    const lastPagePath = treeCalc.getPath(treeCalc.maxPageNumber);
    expect(lastPagePath.length).toBe(expectedHeight - 1);
    testCase.verifyPath(lastPagePath);
  }
});
