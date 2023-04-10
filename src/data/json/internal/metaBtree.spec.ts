import { expect, test } from "vitest";
import { allocateAndInitBtreeRootPage } from "../../btree/btree";
import { PageProviderForWrite } from "../../btree/pageProvider";
import { JsonEventType, JsonPath, JSON_FALSE, JSON_NULL, JSON_STRING } from "../jsonEvents";
import {
  jsonPathAndTypeToNumber,
  JsonPathAndTypeToNumberCache,
  numberToJsonPathAndType,
  NumberToJsonPathAndTypeCache,
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
  const path2: JsonPath = { key: null, parent: path1 };
  const path3: JsonPath = { key: "bar", parent: path2 };
  const path4: JsonPath = { key: "baz" };

  const pathAndTypes: { path?: JsonPath; type: JsonEventType }[] = [
    { type: JSON_FALSE },
    { type: JSON_NULL },
    { type: JSON_STRING },
    { type: JSON_FALSE, path: path1 },
    { type: JSON_FALSE, path: path2 },
    { type: JSON_FALSE, path: path3 },
    { type: JSON_FALSE, path: path4 },
    { type: JSON_STRING, path: path1 },
    { type: JSON_STRING, path: path2 },
    { type: JSON_STRING, path: path3 },
    { type: JSON_STRING, path: path4 },
  ];

  const numbers: number[] = [];

  expect(() => numberToJsonPathAndType(pageProvider.getPage, metaRootPageNumber, 1)).toThrow();

  const toNumberCache: JsonPathAndTypeToNumberCache = new Map();
  for (const pathAndType of pathAndTypes) {
    const params = [pageProvider, metaRootPageNumber, pathAndType.path, pathAndType.type] as const;
    const number = jsonPathAndTypeToNumber(...params);
    numbers.push(number);
    expect(jsonPathAndTypeToNumber(...params)).toBe(number);
    expect(jsonPathAndTypeToNumber(...params, toNumberCache)).toBe(number);
    expect(jsonPathAndTypeToNumber(...params, toNumberCache)).toBe(number);
  }

  const fromNumberCache: NumberToJsonPathAndTypeCache = new Map();
  for (let i = 0; i < pathAndTypes.length; i++) {
    const pathAndType = pathAndTypes[i];
    const number = numbers[i];
    expect(jsonPathAndTypeToNumber(pageProvider, metaRootPageNumber, pathAndType.path, pathAndType.type)).toBe(number);
    expect(
      jsonPathAndTypeToNumber(pageProvider, metaRootPageNumber, pathAndType.path, pathAndType.type, toNumberCache)
    ).toBe(number);

    expect(numberToJsonPathAndType(pageProvider.getPage, metaRootPageNumber, number)).toEqual(pathAndType);

    const cachedResult = numberToJsonPathAndType(pageProvider.getPage, metaRootPageNumber, number, fromNumberCache);
    expect(cachedResult).toEqual(pathAndType);
    expect(numberToJsonPathAndType(pageProvider.getPage, metaRootPageNumber, number, fromNumberCache)).toBe(
      cachedResult
    );
  }
});
