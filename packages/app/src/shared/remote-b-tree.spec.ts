import { describe, expect, test } from "vitest";
import { BTreeSet } from "./b-tree-set";

function testInsertAndDelete(treeSet: BTreeSet, insertElements: string[], deleteElements: string[]) {
  expect(treeSet.getKeyCount().value).toBe(0);
  expect(treeSet.data.size).toBe(1); // root node

  for (const element of insertElements) {
    expect(treeSet.contains(element).value).toBe(false);
    expect(treeSet.insert(element).value).toBe(true);
    expect(treeSet.contains(element).value).toBe(true);
  }

  expect(treeSet.getKeyCount().value).toBe(insertElements.length);

  expect(treeSet.simpleScan().value).toStrictEqual([...insertElements].sort());

  for (const element of insertElements) {
    expect(treeSet.insert(element).value).toBe(false);
    expect(treeSet.contains(element).value).toBe(true);
  }

  for (const element of deleteElements) {
    expect(treeSet.contains(element).value).toBe(true);
    expect(treeSet.delete(element).value).toBe(true);
    expect(treeSet.contains(element).value).toBe(false);
  }

  expect(treeSet.getKeyCount().value).toBe(0);
  expect(treeSet.data.size).toBe(1);

  for (const element of deleteElements) {
    expect(treeSet.delete(element).value).toBe(false);
    expect(treeSet.contains(element).value).toBe(false);
  }
}

interface TestParameters {
  maxNodeSizes: number[];
  elements: string[];
}

function testTree(title: string, parameters: TestParameters) {
  for (const maxNodeSize of parameters.maxNodeSizes) {
    test(`${title} and maxNodeSize ${maxNodeSize}`, () => {
      // use BTreeSet as a wrapper, it already provides some useful helper functionality
      const treeSet = new BTreeSet(maxNodeSize);

      const elements = parameters.elements;
      const reverseElements = [...elements].reverse();
      const sortedElements = [...elements].sort();
      const elementLists = [elements, reverseElements, sortedElements];
      // test with all combinations
      for (const insertElements of elementLists) {
        for (const deleteElements of elementLists) {
          testInsertAndDelete(treeSet, insertElements, deleteElements);
        }
      }
    });
  }
}

const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 -.,+?!\"'$%&/()=";
function generateRandomString(): string {
  const length = Math.floor(Math.random() * 15);
  let result = "";
  for (let i = 0; i < length; ++i) {
    result += characters.charAt(Math.floor(Math.random() * characters.length));
  }
  return result;
}

describe("RemoteBTree", () => {
  describe("insert and delete", () => {
    testTree("should work with fixed simple entries", {
      maxNodeSizes: [30, 50, 200],
      elements: "1,v,asd,hallo,abc,reg,32,fj443,23,35,36,4,,75624,57567,a,b,t,e,g,ju,o".split(","),
    });

    const seen: Record<string, boolean> = {};
    const elements: string[] = [];
    while (elements.length < 500) {
      const element = generateRandomString();
      if (!seen.hasOwnProperty(element)) {
        elements.push(element);
        seen[element] = true;
      }
    }
    testTree("should work with 500 random entries", {
      maxNodeSizes: [30, 50, 200, 500, 1000, 10000],
      elements,
    });
  });
});
