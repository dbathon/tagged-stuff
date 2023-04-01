import { describe, expect, test } from "vitest";
import { buildJsonFromEvents, Json, JsonEvent, produceJsonEvents } from "./jsonEvents";

describe("jsonEvents", () => {
  const testValues: Json[] = [
    null,
    true,
    false,
    "some string",
    1234,
    456.78,
    {
      a: { b: [[], [1, 2, 3], [4, 5], 6, null, 7, {}, [8, 9], [[[[[42]]]]]] },
      b: "bla",
      c: [4, 2, 1],
    },
    [],
    {},
    [1, "2", { c: 3 }, { c: 4 }, 5, { c: 6, d: "aa", e: [1, 2, 4], f: "foo" }, {}],
  ];

  test("produceJsonEvents() and buildJsonFromEvents()", () => {
    for (const testValue of testValues) {
      const events: JsonEvent[] = [];
      produceJsonEvents(testValue, (type, path, value) => {
        const event: JsonEvent = { type, path, value };
        events.push(event);
      });
      const jsonFromEvents = buildJsonFromEvents(events);
      expect(jsonFromEvents).toStrictEqual(testValue);
    }
  });
});
