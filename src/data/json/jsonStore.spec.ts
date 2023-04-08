import { expect, test } from "vitest";
import { PageAccessDuringTransaction } from "../page-store/PageAccessDuringTransaction";
import { PageData } from "../page-store/PageData";
import { deleteJson, queryJson, saveJson } from "./jsonStore";

function createPageAccess(pageSize: number): PageAccessDuringTransaction {
  const pages = new Map<number, PageData>();
  function get(pageNumber: number): PageData {
    let result = pages.get(pageNumber);
    if (!result) {
      result = new PageData(new ArrayBuffer(pageSize));
      pages.set(pageNumber, result);
    }
    return result;
  }
  return {
    get,
    getForUpdate: get,
  };
}

interface WithId {
  id?: number;
}

interface Foo extends WithId {
  active: boolean;
  foo: string;
  count: number;
}

interface Bar extends WithId {
  barStrings: string[];
  barNumbers: number[];
  barFlags: boolean[];
}

test("jsonStore", () => {
  const pageAccess = createPageAccess(400);

  expect(queryJson<Foo>(pageAccess.get, "foo")).toEqual([]);
  expect(queryJson<Bar>(pageAccess.get, "bar")).toEqual([]);

  const foo0: Foo = {
    active: true,
    foo: "foo1",
    count: 23,
  };

  saveJson(pageAccess, "foo", foo0);
  expect(foo0.id).toBe(0);

  expect(queryJson<Foo>(pageAccess.get, "foo")).toEqual([foo0]);

  const foo1: Foo = {
    active: false,
    foo: "foo2",
    count: 42,
  };
  saveJson(pageAccess, "foo", foo1);
  expect(foo1.id).toBe(1);

  expect(queryJson<Foo>(pageAccess.get, "foo")).toEqual([foo0, foo1]);

  foo0.count += 5;
  saveJson(pageAccess, "foo", foo0);
  expect(foo0.id).toBe(0);
  expect(queryJson<Foo>(pageAccess.get, "foo")).toEqual([foo0, foo1]);

  expect(queryJson<Foo>(pageAccess.get, "foo", { minId: 1 })).toEqual([foo1]);
  expect(queryJson<Foo>(pageAccess.get, "foo", { minId: 2 })).toEqual([]);
  expect(queryJson<Foo>(pageAccess.get, "foo", { maxResults: 1 })).toEqual([foo0]);

  expect(deleteJson(pageAccess, "foo", 0)).toBe(true);
  expect(deleteJson(pageAccess, "foo", 0)).toBe(false);

  const bar0: Bar = {
    barStrings: ["a", "b", "c"],
    barNumbers: [1, 3, 5, 7, 9],
    barFlags: [true, false, false],
  };

  saveJson(pageAccess, "bar", bar0);
  expect(bar0.id).toBe(0);
  expect(queryJson<Bar>(pageAccess.get, "bar")).toEqual([bar0]);
});
