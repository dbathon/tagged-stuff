import { expect, test } from "vitest";
import { allocateAndInitBtreeRootPage } from "../../btree/btree";
import { type PageProviderForWrite } from "../../btree/pageProvider";
import { type JsonPath } from "../jsonEvents";
import {
  type JsonPathToNumberCache,
  type NumberToJsonPathCache,
  jsonPathToNumber,
  numberToJsonPath,
} from "./metaBtree";

function createPageProviderForWrite(pageSize: number): PageProviderForWrite {
  const pages: Uint8Array[] = [];
  function getPage(pageNumber: number): Uint8Array {
    const result = pages[pageNumber];
    if (!result) {
      throw new Error("page does not exist: " + pageNumber);
    }
    return result;
  }
  return {
    getPage,
    getPageForUpdate: getPage,
    allocateNewPage() {
      pages.push(new Uint8Array(pageSize));
      return pages.length - 1;
    },
    releasePage(pageNumber: number) {},
  };
}

test("metaBtree", () => {
  const pageProvider = createPageProviderForWrite(400);
  const metaRootPageNumber = allocateAndInitBtreeRootPage(pageProvider);

  const path1: JsonPath = { key: "foo" };
  const path2: JsonPath = { key: 0, parent: path1 };
  const path3: JsonPath = { key: "bar", parent: path2 };
  const path4: JsonPath = { key: "baz" };

  const paths: JsonPath[] = [path1, path2, path3, path4];

  const numbers: number[] = [];

  expect(() => numberToJsonPath(pageProvider.getPage, metaRootPageNumber, 1)).toThrow();

  const toNumberCache: JsonPathToNumberCache = new Map();
  for (const path of paths) {
    const params = [pageProvider, metaRootPageNumber, path] as const;
    const number = jsonPathToNumber(...params);
    numbers.push(number);
    expect(jsonPathToNumber(...params)).toBe(number);
    expect(jsonPathToNumber(...params, toNumberCache)).toBe(number);
    expect(jsonPathToNumber(...params, toNumberCache)).toBe(number);
  }

  const fromNumberCache: NumberToJsonPathCache = new Map();
  for (let i = 0; i < paths.length; i++) {
    const path = paths[i];
    const number = numbers[i];
    expect(jsonPathToNumber(pageProvider, metaRootPageNumber, path)).toBe(number);
    expect(jsonPathToNumber(pageProvider, metaRootPageNumber, path, toNumberCache)).toBe(number);

    expect(numberToJsonPath(pageProvider.getPage, metaRootPageNumber, number)).toEqual(path);

    const cachedResult = numberToJsonPath(pageProvider.getPage, metaRootPageNumber, number, fromNumberCache);
    expect(cachedResult).toEqual(path);
    expect(numberToJsonPath(pageProvider.getPage, metaRootPageNumber, number, fromNumberCache)).toBe(cachedResult);
  }
});
