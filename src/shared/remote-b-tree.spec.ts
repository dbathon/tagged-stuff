import { strictEqual } from "assert";
import { describe } from "mocha";
import { BTreeSet } from "./b-tree-set";

function testInsertAndDelete(treeSet: BTreeSet, insertElements: string[], deleteElements: string[]) {
  strictEqual(treeSet.getSize().value, 0);
  strictEqual(treeSet.data.size, 1); // root node

  for (const element of insertElements) {
    strictEqual(treeSet.contains(element).value, false);
    strictEqual(treeSet.insert(element).value, true);
    strictEqual(treeSet.contains(element).value, true);
  }

  strictEqual(treeSet.getSize().value, insertElements.length);

  for (const element of insertElements) {
    strictEqual(treeSet.insert(element).value, false);
    strictEqual(treeSet.contains(element).value, true);
  }

  for (const element of deleteElements) {
    strictEqual(treeSet.contains(element).value, true);
    strictEqual(treeSet.delete(element).value, true);
    strictEqual(treeSet.contains(element).value, false);
  }

  strictEqual(treeSet.getSize().value, 0);
  strictEqual(treeSet.data.size, 1);

  for (const element of deleteElements) {
    strictEqual(treeSet.delete(element).value, false);
    strictEqual(treeSet.contains(element).value, false);
  }
}

interface TestParameters {
  orders: number[];
  elements: string[];
}

function testTree(parameters: TestParameters) {
  for (const order of parameters.orders) {
    // use BTreeSet as a wrapper, it already provides some useful helper functionality
    const treeSet = new BTreeSet(order);

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
    it("should work with fixed simple entries", () => {
      testTree({
        orders: [3, 5, 20],
        elements: "1,v,asd,hallo,abc,reg,32,fj443,23,35,36,4,,75624,57567,a,b,t,e,g,ju,o".split(",")
      });
    });

    it("should work with 500 random entries", () => {
      const seen: Record<string, boolean> = {};
      const elements: string[] = [];
      while (elements.length < 500) {
        const element = generateRandomString();
        if (!seen.hasOwnProperty(element)) {
          elements.push(element);
          seen[element] = true;
        }
      }
      testTree({
        orders: [3, 5, 20, 50, 100, 1000],
        elements
      });
    });
  });
});
