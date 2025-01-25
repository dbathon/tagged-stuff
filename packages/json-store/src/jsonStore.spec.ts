import { assert, test } from "vitest";
import { type PageAccessDuringTransaction } from "page-store";
import { countJson, deleteJson, queryJson, saveJson } from "./jsonStore";
import { type QueryParameters } from "./queryTypes";
import { jsonEquals } from "shared-util";

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

  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo" }), []));
  assert(jsonEquals(queryJson<Bar>(pageAccess.get, { table: "bar" }), []));
  assert(countJson(pageAccess.get, { table: "bar" }) === 0);

  const foo0: Foo = {
    active: true,
    foo: "foo1",
    count: 23,
  };

  saveJson(pageAccess, "foo", foo0);
  assert(foo0.id === 0);

  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo" }), [foo0]));

  const foo1: Foo = {
    active: false,
    foo: "foo2",
    count: 42,
  };
  saveJson(pageAccess, "foo", foo1);
  assert(foo1.id === 1);

  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo" }), [foo0, foo1]));
  assert(countJson(pageAccess.get, { table: "foo" }) === 2);

  foo0.count += 5;
  saveJson(pageAccess, "foo", foo0);
  assert(foo0.id === 0);
  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo" }), [foo0, foo1]));

  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo", filter: ["id >=", 1] }), [foo1]));
  assert(countJson(pageAccess.get, { table: "foo", filter: ["id >=", 1] }) === 1);
  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo", filter: ["id >=", 2] }), []));
  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo", limit: 1 }), [foo0]));

  assert(deleteJson(pageAccess, "foo", 0) === true);
  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo", limit: 1 }), [foo1]));
  assert(deleteJson(pageAccess, "foo", 0) === false);
  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo", limit: 1 }), [foo1]));

  // "restore" foo0
  saveJson(pageAccess, "foo", foo0);
  assert(foo0.id === 0);
  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo", limit: 1 }), [foo0]));

  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo", filter: ["id >=", 42] }), []));

  // save an entry with a given id
  const foo42: Foo = {
    id: 42,
    active: false,
    foo: "foo42",
    count: 42,
  };
  saveJson(pageAccess, "foo", foo42);
  assert(foo42.id === 42);
  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo", filter: ["id >=", 42] }), [foo42]));

  // next id should now be 43
  const foo43: Foo = {
    active: false,
    foo: "foo43",
    count: 43,
  };
  saveJson(pageAccess, "foo", foo43);
  assert(foo43.id === 43);
  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo", filter: ["id >=", 42] }), [foo42, foo43]));

  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo" }), [foo0, foo1, foo42, foo43]));

  // some more deletes
  assert(deleteJson(pageAccess, "foo", 1) === true);
  assert(deleteJson(pageAccess, "foo", 1) === false);
  assert(deleteJson(pageAccess, "foo", 2) === false);
  assert(deleteJson(pageAccess, "foo", 42) === true);
  assert(deleteJson(pageAccess, "foo", 1) === false);
  assert(jsonEquals(queryJson<Foo>(pageAccess.get, { table: "foo" }), [foo0, foo43]));

  const bar0: Bar = {
    barStrings: ["a", "b", "c"],
    barNumbers: [1, 3, 5, 7, 9],
    barFlags: [true, false, false],
  };

  saveJson(pageAccess, "bar", bar0);
  assert(bar0.id === 0);
  assert(jsonEquals(queryJson<Bar>(pageAccess.get, { table: "bar" }), [bar0]));

  const large0: Large = {
    large: "x".repeat(5000),
  };
  saveJson(pageAccess, "large", large0);
  assert(large0.id === 0);
  assert(jsonEquals(queryJson<Large>(pageAccess.get, { table: "large" }), [large0]));

  // test update of large entry
  large0.large = "x".repeat(5000) + "y";
  large0.a = 123;
  large0.b = "foo";
  saveJson(pageAccess, "large", large0);
  assert(large0.id === 0);
  assert(jsonEquals(queryJson<Large>(pageAccess.get, { table: "large" }), [large0]));

  const large1: Large = {
    large: "y".repeat(6000),
    a: 1234.5465656,
  };
  saveJson(pageAccess, "large", large1);
  assert(large1.id === 1);
  assert(jsonEquals(queryJson<Large>(pageAccess.get, { table: "large" }), [large0, large1]));
  assert(countJson(pageAccess.get, { table: "large" }) === 2);

  // empty object should work
  const empty: WithId = {};
  saveJson(pageAccess, "empty", empty);
  assert(empty.id === 0);
  assert(jsonEquals(queryJson(pageAccess.get, { table: "empty" }), [empty]));
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
    assert(entity.id !== undefined);

    entities.push(entity);
  }

  // sort by id
  entities.sort((a, b) => a.id! - b.id!);

  function testQueryAndCount(parameters: QueryParameters, expected: TestEntity[]) {
    const fullResult = queryJson<TestEntity>(pageAccess.get, parameters);
    assert(jsonEquals(fullResult, expected));

    const ids = queryJson(pageAccess.get, parameters, "onlyId");
    assert(
      jsonEquals(
        ids,
        expected.map((e) => e.id),
      ),
    );

    const count = countJson(pageAccess.get, parameters);
    assert(count === expected.length);

    if (parameters.offset === undefined && parameters.limit === undefined) {
      const halfLength = expected.length >>> 1;
      testQueryAndCount(
        {
          limit: halfLength,
          ...parameters,
        },
        expected.slice(0, halfLength),
      );
      testQueryAndCount(
        {
          offset: halfLength,
          ...parameters,
        },
        expected.slice(halfLength),
      );

      const quarterLength = halfLength >>> 1;
      testQueryAndCount(
        {
          offset: quarterLength,
          limit: halfLength,
          ...parameters,
        },
        expected.slice(quarterLength, quarterLength + halfLength),
      );

      const doubleLength = expected.length * 2;
      testQueryAndCount(
        {
          limit: doubleLength,
          ...parameters,
        },
        expected,
      );
      testQueryAndCount(
        {
          offset: halfLength,
          limit: doubleLength,
          ...parameters,
        },
        expected.slice(halfLength),
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
    even.filter((e) => e.i >= 42),
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
    fizzAndBuzz,
  );
  testQueryAndCount(
    {
      table,
      filter: [
        ["fizz =", true],
        ["tags[] =", "buzz"],
      ],
    },
    fizzAndBuzz,
  );
  testQueryAndCount(
    {
      table,
      filter: [
        ["tags[] =", "fizz"],
        ["tags[] in", ["buzz"]],
      ],
    },
    fizzAndBuzz,
  );

  const fizzOrBuzz = entities.filter((e) => e.fizz || e.buzz);
  testQueryAndCount(
    {
      table,
      filter: ["or", ["fizz =", true], ["buzz =", true]],
    },
    fizzOrBuzz,
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
    fizzOrBuzz,
  );
  testQueryAndCount(
    {
      table,
      filter: ["or", ["fizz =", true], ["tags[] =", "buzz"]],
    },
    fizzOrBuzz,
  );
  testQueryAndCount(
    {
      table,
      filter: ["or", ["tags[] =", "fizz"], ["tags[] in", ["buzz"]]],
    },
    fizzOrBuzz,
  );
  testQueryAndCount(
    {
      table,
      filter: ["tags[] in", ["fizz", "buzz"]],
    },
    fizzOrBuzz,
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
    assert(entity.id !== undefined);
  }

  assert(jsonEquals(queryJson(pageAccess.get, { table }), entities));
  // test that sorting by x does not change the order
  assert(jsonEquals(queryJson(pageAccess.get, { table, orderBy: ["x"] }), entities));
});
