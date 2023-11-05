import { expect, test } from "vitest";
import { PageAccessDuringTransaction } from "../page-store/PageAccessDuringTransaction";
import { countJson, deleteJson, queryJson, saveJson } from "./jsonStore";
import { QueryParameters } from "./queryTypes";

function createPageAccess(pageSize: number): PageAccessDuringTransaction {
  const pages = new Map<number, Uint8Array>();
  function get(pageNumber: number): Uint8Array {
    let result = pages.get(pageNumber);
    if (!result) {
      result = new Uint8Array(pageSize);
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

  expect(queryJson<Foo>(pageAccess.get, { table: "foo" })).toEqual([]);
  expect(queryJson<Bar>(pageAccess.get, { table: "bar" })).toEqual([]);
  expect(countJson(pageAccess.get, { table: "bar" })).toEqual(0);

  const foo0: Foo = {
    active: true,
    foo: "foo1",
    count: 23,
  };

  saveJson(pageAccess, "foo", foo0);
  expect(foo0.id).toBe(0);

  expect(queryJson<Foo>(pageAccess.get, { table: "foo" })).toEqual([foo0]);

  const foo1: Foo = {
    active: false,
    foo: "foo2",
    count: 42,
  };
  saveJson(pageAccess, "foo", foo1);
  expect(foo1.id).toBe(1);

  expect(queryJson<Foo>(pageAccess.get, { table: "foo" })).toEqual([foo0, foo1]);
  expect(countJson(pageAccess.get, { table: "foo" })).toEqual(2);

  foo0.count += 5;
  saveJson(pageAccess, "foo", foo0);
  expect(foo0.id).toBe(0);
  expect(queryJson<Foo>(pageAccess.get, { table: "foo" })).toEqual([foo0, foo1]);

  expect(queryJson<Foo>(pageAccess.get, { table: "foo", filter: ["id >=", 1] })).toEqual([foo1]);
  expect(countJson(pageAccess.get, { table: "foo", filter: ["id >=", 1] })).toEqual(1);
  expect(queryJson<Foo>(pageAccess.get, { table: "foo", filter: ["id >=", 2] })).toEqual([]);
  expect(queryJson<Foo>(pageAccess.get, { table: "foo", limit: 1 })).toEqual([foo0]);

  expect(deleteJson(pageAccess, "foo", 0)).toBe(true);
  expect(queryJson<Foo>(pageAccess.get, { table: "foo", limit: 1 })).toEqual([foo1]);
  expect(deleteJson(pageAccess, "foo", 0)).toBe(false);
  expect(queryJson<Foo>(pageAccess.get, { table: "foo", limit: 1 })).toEqual([foo1]);

  // "restore" foo0
  saveJson(pageAccess, "foo", foo0);
  expect(foo0.id).toBe(0);
  expect(queryJson<Foo>(pageAccess.get, { table: "foo", limit: 1 })).toEqual([foo0]);

  expect(queryJson<Foo>(pageAccess.get, { table: "foo", filter: ["id >=", 42] })).toEqual([]);

  // save an entry with a given id
  const foo42: Foo = {
    id: 42,
    active: false,
    foo: "foo42",
    count: 42,
  };
  saveJson(pageAccess, "foo", foo42);
  expect(foo42.id).toBe(42);
  expect(queryJson<Foo>(pageAccess.get, { table: "foo", filter: ["id >=", 42] })).toEqual([foo42]);

  // next id should now be 43
  const foo43: Foo = {
    active: false,
    foo: "foo43",
    count: 43,
  };
  saveJson(pageAccess, "foo", foo43);
  expect(foo43.id).toBe(43);
  expect(queryJson<Foo>(pageAccess.get, { table: "foo", filter: ["id >=", 42] })).toEqual([foo42, foo43]);

  expect(queryJson<Foo>(pageAccess.get, { table: "foo" })).toEqual([foo0, foo1, foo42, foo43]);

  // some more deletes
  expect(deleteJson(pageAccess, "foo", 1)).toBe(true);
  expect(deleteJson(pageAccess, "foo", 1)).toBe(false);
  expect(deleteJson(pageAccess, "foo", 2)).toBe(false);
  expect(deleteJson(pageAccess, "foo", 42)).toBe(true);
  expect(deleteJson(pageAccess, "foo", 1)).toBe(false);
  expect(queryJson<Foo>(pageAccess.get, { table: "foo" })).toEqual([foo0, foo43]);

  const bar0: Bar = {
    barStrings: ["a", "b", "c"],
    barNumbers: [1, 3, 5, 7, 9],
    barFlags: [true, false, false],
  };

  saveJson(pageAccess, "bar", bar0);
  expect(bar0.id).toBe(0);
  expect(queryJson<Bar>(pageAccess.get, { table: "bar" })).toEqual([bar0]);

  const large0: Large = {
    large: "x".repeat(5000),
  };
  saveJson(pageAccess, "large", large0);
  expect(large0.id).toBe(0);
  expect(queryJson<Large>(pageAccess.get, { table: "large" })).toEqual([large0]);

  // test update of large entry
  large0.large = "x".repeat(5000) + "y";
  large0.a = 123;
  large0.b = "foo";
  saveJson(pageAccess, "large", large0);
  expect(large0.id).toBe(0);
  expect(queryJson<Large>(pageAccess.get, { table: "large" })).toEqual([large0]);

  const large1: Large = {
    large: "y".repeat(6000),
    a: 1234.5465656,
  };
  saveJson(pageAccess, "large", large1);
  expect(large1.id).toBe(1);
  expect(queryJson<Large>(pageAccess.get, { table: "large" })).toEqual([large0, large1]);
  expect(countJson(pageAccess.get, { table: "large" })).toEqual(2);

  // empty object should work
  const empty: WithId = {};
  saveJson(pageAccess, "empty", empty);
  expect(empty.id).toBe(0);
  expect(queryJson(pageAccess.get, { table: "empty" })).toEqual([empty]);
});

interface TestEntity extends WithId {
  i: number;
  even: boolean;
  evenOrUndefined?: boolean;
  fizz: boolean;
  buzz: boolean;
  tags: string[];
}

test("queryJson", () => {
  const pageAccess = createPageAccess(4096);
  const table = "test";
  const entities: TestEntity[] = [];

  for (let i = 0; i < 1000; i++) {
    const entity: TestEntity = {
      i,
      even: i % 2 === 0,
      fizz: i % 3 === 0,
      buzz: i % 5 === 0,
      tags: [],
    };
    if (entity.even) {
      entity.evenOrUndefined = true;
      entity.tags.push("even");
    }
    if (entity.fizz) {
      entity.tags.push("fizz");
    }
    if (entity.buzz) {
      entity.tags.push("buzz");
    }

    if (i >= 500) {
      // explicitly set some ids
      entity.id = 2000 - i;
    }

    saveJson(pageAccess, table, entity);
    expect(entity.id).toBeDefined();

    entities.push(entity);
  }

  // sort by id
  entities.sort((a, b) => a.id! - b.id!);

  function testQueryAndCount(parameters: QueryParameters, expected: TestEntity[]) {
    const fullResult = queryJson<TestEntity>(pageAccess.get, parameters);
    expect(fullResult).toEqual(expected);

    const ids = queryJson(pageAccess.get, parameters, "onlyId");
    expect(ids).toEqual(expected.map((e) => e.id));

    const count = countJson(pageAccess.get, parameters);
    expect(count).toEqual(expected.length);

    if (parameters.offset === undefined && parameters.limit === undefined) {
      const halfLength = expected.length >>> 1;
      testQueryAndCount(
        {
          limit: halfLength,
          ...parameters,
        },
        expected.slice(0, halfLength)
      );
      testQueryAndCount(
        {
          offset: halfLength,
          ...parameters,
        },
        expected.slice(halfLength)
      );

      const quarterLength = halfLength >>> 1;
      testQueryAndCount(
        {
          offset: quarterLength,
          limit: halfLength,
          ...parameters,
        },
        expected.slice(quarterLength, quarterLength + halfLength)
      );

      const doubleLength = expected.length * 2;
      testQueryAndCount(
        {
          limit: doubleLength,
          ...parameters,
        },
        expected
      );
      testQueryAndCount(
        {
          offset: halfLength,
          limit: doubleLength,
          ...parameters,
        },
        expected.slice(halfLength)
      );
    }
  }

  testQueryAndCount({ table }, entities);

  const even = entities.filter((e) => e.even);
  testQueryAndCount({ table, filter: ["even", "=", true] }, even);
  testQueryAndCount({ table, filter: ["even =", true] }, even);
  testQueryAndCount(
    {
      table,
      filter: [
        ["even =", true],
        ["i >=", 42],
      ],
    },
    even.filter((e) => e.i >= 42)
  );
  testQueryAndCount({ table, filter: ["evenOrUndefined =", true] }, even);
  testQueryAndCount({ table, filter: ["evenOrUndefined is", "boolean"] }, even);
  testQueryAndCount({ table, filter: ["evenOrUndefined match", (even) => !!even] }, even);
  testQueryAndCount({ table, filter: ["tags[] =", "even"] }, even);
  testQueryAndCount({ table, filter: ["tags", 0, "=", "even"] }, even);
  testQueryAndCount({ table, extraFilter: (e) => (e as any).even }, even);

  even.sort((a, b) => a.i - b.i);
  testQueryAndCount({ table, filter: ["even", "=", true], orderBy: ["i"] }, even);
  testQueryAndCount({ table, filter: ["even", "=", true], orderBy: ["i", "id"] }, even);
  testQueryAndCount({ table, filter: ["even", "=", true], orderBy: [["i"], ["id"]] }, even);

  const fizzAndBuzz = entities.filter((e) => e.fizz && e.buzz);
  testQueryAndCount(
    {
      table,
      filter: [
        ["fizz =", true],
        ["buzz =", true],
      ],
    },
    fizzAndBuzz
  );
  testQueryAndCount(
    {
      table,
      filter: [
        ["fizz =", true],
        ["tags[] =", "buzz"],
      ],
    },
    fizzAndBuzz
  );
  testQueryAndCount(
    {
      table,
      filter: [
        ["tags[] =", "fizz"],
        ["tags[] in", ["buzz"]],
      ],
    },
    fizzAndBuzz
  );

  const fizzOrBuzz = entities.filter((e) => e.fizz || e.buzz);
  testQueryAndCount(
    {
      table,
      filter: ["or", ["fizz =", true], ["buzz =", true]],
    },
    fizzOrBuzz
  );
  testQueryAndCount(
    {
      table,
      filter: [
        ["or", ["fizz =", true], ["buzz =", true]],
        // add some extra and-ed conditions that don't change the result
        [
          ["id is", "number"],
          ["i is", "number"],
        ],
      ],
    },
    fizzOrBuzz
  );
  testQueryAndCount(
    {
      table,
      filter: ["or", ["fizz =", true], ["tags[] =", "buzz"]],
    },
    fizzOrBuzz
  );
  testQueryAndCount(
    {
      table,
      filter: ["or", ["tags[] =", "fizz"], ["tags[] in", ["buzz"]]],
    },
    fizzOrBuzz
  );
  testQueryAndCount(
    {
      table,
      filter: ["tags[] in", ["fizz", "buzz"]],
    },
    fizzOrBuzz
  );
});

interface WithUnknown extends WithId {
  x: unknown;
}

test("queryJson sorting", () => {
  const pageAccess = createPageAccess(4096);
  const table = "test";
  const entities: WithUnknown[] = [
    { x: -1 },
    { x: 0 },
    { x: 1 },
    { x: "a" },
    { x: "b" },
    { x: false },
    { x: true },
    { x: null },
    { x: undefined },
  ];
  for (const entity of entities) {
    saveJson(pageAccess, table, entity);
    expect(entity.id).toBeDefined();
  }

  expect(queryJson(pageAccess.get, { table })).toEqual(entities);
  // test that sorting by x does not change the order
  expect(queryJson(pageAccess.get, { table, orderBy: ["x"] })).toEqual(entities);
});
