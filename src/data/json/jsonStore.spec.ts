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

interface Large extends WithId {
  large: string;
  a?: number;
  b?: string;
}

test("jsonStore", () => {
  const pageAccess = createPageAccess(4096);

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
  expect(queryJson<Foo>(pageAccess.get, "foo", { maxResults: 1 })).toEqual([foo1]);
  expect(deleteJson(pageAccess, "foo", 0)).toBe(false);
  expect(queryJson<Foo>(pageAccess.get, "foo", { maxResults: 1 })).toEqual([foo1]);

  // "restore" foo0
  saveJson(pageAccess, "foo", foo0);
  expect(foo0.id).toBe(0);
  expect(queryJson<Foo>(pageAccess.get, "foo", { maxResults: 1 })).toEqual([foo0]);

  expect(queryJson<Foo>(pageAccess.get, "foo", { minId: 42 })).toEqual([]);

  // save an entry with a given id
  const foo42: Foo = {
    id: 42,
    active: false,
    foo: "foo42",
    count: 42,
  };
  saveJson(pageAccess, "foo", foo42);
  expect(foo42.id).toBe(42);
  expect(queryJson<Foo>(pageAccess.get, "foo", { minId: 42 })).toEqual([foo42]);

  // next id should now be 43
  const foo43: Foo = {
    active: false,
    foo: "foo43",
    count: 43,
  };
  saveJson(pageAccess, "foo", foo43);
  expect(foo43.id).toBe(43);
  expect(queryJson<Foo>(pageAccess.get, "foo", { minId: 42 })).toEqual([foo42, foo43]);

  expect(queryJson<Foo>(pageAccess.get, "foo")).toEqual([foo0, foo1, foo42, foo43]);

  // some more deletes
  expect(deleteJson(pageAccess, "foo", 1)).toBe(true);
  expect(deleteJson(pageAccess, "foo", 1)).toBe(false);
  expect(deleteJson(pageAccess, "foo", 2)).toBe(false);
  expect(deleteJson(pageAccess, "foo", 42)).toBe(true);
  expect(deleteJson(pageAccess, "foo", 1)).toBe(false);
  expect(queryJson<Foo>(pageAccess.get, "foo")).toEqual([foo0, foo43]);

  const bar0: Bar = {
    barStrings: ["a", "b", "c"],
    barNumbers: [1, 3, 5, 7, 9],
    barFlags: [true, false, false],
  };

  saveJson(pageAccess, "bar", bar0);
  expect(bar0.id).toBe(0);
  expect(queryJson<Bar>(pageAccess.get, "bar")).toEqual([bar0]);

  const large0: Large = {
    large: "x".repeat(5000),
  };
  saveJson(pageAccess, "large", large0);
  expect(large0.id).toBe(0);
  expect(queryJson<Large>(pageAccess.get, "large")).toEqual([large0]);

  // test update of large entry
  large0.large = "x".repeat(5000) + "y";
  large0.a = 123;
  large0.b = "foo";
  saveJson(pageAccess, "large", large0);
  expect(large0.id).toBe(0);
  expect(queryJson<Large>(pageAccess.get, "large")).toEqual([large0]);

  const large1: Large = {
    large: "y".repeat(6000),
    a: 1234.5465656,
  };
  saveJson(pageAccess, "large", large1);
  expect(large1.id).toBe(1);
  expect(queryJson<Large>(pageAccess.get, "large")).toEqual([large0, large1]);
});
