import { expect, test } from "vitest";
import { allocateAndInitBtreeRootPage } from "../../btree/btree";
import { PageProviderForWrite } from "../../btree/pageProvider";
import { JsonEventType, JsonPath, JSON_FALSE, JSON_NULL, JSON_STRING } from "../jsonEvents";
import { jsonPathAndTypeToNumber, numberToJsonPathAndType } from "./metaBtree";

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

  for (const pathAndType of pathAndTypes) {
    numbers.push(jsonPathAndTypeToNumber(pageProvider, metaRootPageNumber, pathAndType.path, pathAndType.type));
  }

  for (let i = 0; i < pathAndTypes.length; i++) {
    const pathAndType = pathAndTypes[i];
    const number = numbers[i];
    expect(jsonPathAndTypeToNumber(pageProvider, metaRootPageNumber, pathAndType.path, pathAndType.type)).toBe(number);
    expect(jsonPathAndTypeToNumber(pageProvider, metaRootPageNumber, pathAndType.path, pathAndType.type)).toBe(number);

    expect(numberToJsonPathAndType(pageProvider.getPage, metaRootPageNumber, number)).toEqual(pathAndType);
  }
});
