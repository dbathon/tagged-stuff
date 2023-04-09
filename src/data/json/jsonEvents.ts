import { assert } from "../misc/assert";

/**
 * The functions in this file allow producing an "event stream" from a JSON value and then in reverse also allow
 * reconstructing the JSON value from the events.
 *
 * This can be useful for serializing JSON.
 *
 * The exact behavior of the functions regarding allowed values and conversions differ a bit from JSON.stringify():
 * * Infinity and NaN are allowed for numbers and will produce events with those values
 * * "non-JSON" values (including undefined) are not allowed and will cause an Error to be thrown
 * * undefined as the value of an object entry is allowed and will just be skipped
 * * toJSON() is not considered/used
 */

// the following are the types of JSON events that can occur
export const JSON_NULL = 0;
export const JSON_TRUE = 1;
export const JSON_FALSE = 2;
export const JSON_NUMBER = 3;
export const JSON_STRING = 4;
// empty array and empty object need to be handled specifically, because there are no children to infer them from
export const JSON_EMPTY_ARRAY = 5;
export const JSON_EMPTY_OBJECT = 6;
// events with this type are only emitted if necessary, e.g. between two objects or two arrays
export const JSON_ARRAY_NEW_ELEMENT = 7;

export type JsonEventType =
  | typeof JSON_NULL
  | typeof JSON_TRUE
  | typeof JSON_FALSE
  | typeof JSON_NUMBER
  | typeof JSON_STRING
  | typeof JSON_EMPTY_ARRAY
  | typeof JSON_EMPTY_OBJECT
  | typeof JSON_ARRAY_NEW_ELEMENT;

export interface JsonPath {
  readonly parent?: JsonPath;
  /** key null means the current "object" is an array. */
  readonly key: string | null;
}

export interface JsonEvent {
  readonly path?: JsonPath;
  readonly type: JsonEventType;
  readonly value?: string | number;
}

export type JsonEventConsumer = (type: JsonEventType, path: JsonPath | undefined, value?: string | number) => unknown;

/** This type kind of restricts what can be represented as JSON, but it is not perfect... */
export type Json = null | boolean | number | string | unknown[] | object;

const NEW_ELEMENT_REQUIRED_IF_ARRAY = 1;
const NEW_ELEMENT_REQUIRED_IF_OBJECT = 2;

type ProduceEventsResult = typeof NEW_ELEMENT_REQUIRED_IF_ARRAY | typeof NEW_ELEMENT_REQUIRED_IF_OBJECT | false;

type AttributeFilter = (key: string, parentPath: JsonPath | undefined) => boolean;

function produceEvents(
  jsonValue: Json,
  consumer: JsonEventConsumer,
  filter: AttributeFilter | undefined,
  parentPath: JsonPath | undefined,
  previousArrayEntryResult?: ProduceEventsResult
): ProduceEventsResult {
  if (typeof jsonValue === "object") {
    if (jsonValue === null) {
      consumer(JSON_NULL, parentPath);
    } else if (Array.isArray(jsonValue)) {
      const length = jsonValue.length;
      if (length === 0) {
        consumer(JSON_EMPTY_ARRAY, parentPath);
      } else {
        if (previousArrayEntryResult === NEW_ELEMENT_REQUIRED_IF_ARRAY) {
          consumer(JSON_ARRAY_NEW_ELEMENT, parentPath);
        }
        const path: JsonPath = {
          parent: parentPath,
          key: null,
        };
        let previousResult: ProduceEventsResult | undefined = undefined;
        for (let i = 0; i < length; i++) {
          previousResult = produceEvents(jsonValue[i], consumer, filter, path, previousResult);
        }
        return NEW_ELEMENT_REQUIRED_IF_ARRAY;
      }
    } else {
      let entrySeen = false;
      for (const [key, value] of Object.entries(jsonValue)) {
        if (value !== undefined && (!filter || filter(key, parentPath))) {
          if (!entrySeen) {
            if (previousArrayEntryResult === NEW_ELEMENT_REQUIRED_IF_OBJECT) {
              consumer(JSON_ARRAY_NEW_ELEMENT, parentPath);
            }
            entrySeen = true;
          }
          const path: JsonPath = {
            parent: parentPath,
            key,
          };
          produceEvents(value, consumer, filter, path);
        }
      }
      if (!entrySeen) {
        consumer(JSON_EMPTY_OBJECT, parentPath);
      } else {
        return NEW_ELEMENT_REQUIRED_IF_OBJECT;
      }
    }
  } else if (jsonValue === true) {
    consumer(JSON_TRUE, parentPath);
  } else if (jsonValue === false) {
    consumer(JSON_FALSE, parentPath);
  } else if (typeof jsonValue === "number") {
    consumer(JSON_NUMBER, parentPath, jsonValue);
  } else if (typeof jsonValue === "string") {
    consumer(JSON_STRING, parentPath, jsonValue);
  } else {
    assert(false, "unexpected JSON value");
  }
  return false;
}

export function produceJsonEvents(jsonValue: Json, consumer: JsonEventConsumer, filter?: AttributeFilter): void {
  produceEvents(jsonValue, consumer, filter, undefined);
}

function pathsEqual(a: JsonPath | undefined, b: JsonPath | undefined): boolean {
  if (a === b) {
    return true;
  }
  if (a === undefined || b === undefined || a.key !== b.key) {
    return false;
  }
  return pathsEqual(a.parent, b.parent);
}

function findDirectChildPath(
  currentPath: JsonPath | undefined,
  potentialChildPath: JsonPath | undefined
): JsonPath | undefined {
  let result: JsonPath | undefined = potentialChildPath;
  while (result !== undefined && !pathsEqual(currentPath, result.parent)) {
    result = result.parent;
  }
  return result;
}

function getSimpleValue(event: JsonEvent): Json {
  switch (event.type) {
    case JSON_NULL:
      return null;
    case JSON_TRUE:
      return true;
    case JSON_FALSE:
      return false;
    case JSON_NUMBER:
      assert(typeof event.value === "number", "value missing for NUMBER event");
      return event.value;
    case JSON_STRING:
      assert(typeof event.value === "string", "value missing for STRING event");
      return event.value;
    case JSON_EMPTY_ARRAY:
      return [];
    case JSON_EMPTY_OBJECT:
      return {};
    case JSON_ARRAY_NEW_ELEMENT:
      assert(false, "unexpected NEW_ELEMENT");
  }
}

function buildJson(events: JsonEvent[], index: number, path: JsonPath | undefined): { value: Json; newIndex: number } {
  const length = events.length;
  assert(index < length, "no remaining events");
  let event = events[index];
  if (pathsEqual(event.path, path)) {
    return {
      value: getSimpleValue(event),
      newIndex: index + 1,
    };
  } else {
    let childPath = findDirectChildPath(path, event.path);
    if (childPath) {
      if (childPath.key === null) {
        // value is array
        const value: Json[] = [];
        while (true) {
          if (event.path && pathsEqual(event.path, childPath) && event.type === JSON_ARRAY_NEW_ELEMENT) {
            // just skip this event, nothing specifically needs to be done
            index++;
          } else {
            const childResult = buildJson(events, index, childPath);
            value.push(childResult.value);
            index = childResult.newIndex;
          }
          if (index >= length) {
            break;
          }
          event = events[index];
          childPath = findDirectChildPath(path, event.path);
          if (!childPath || childPath.key !== null) {
            break;
          }
        }

        return {
          value,
          newIndex: index,
        };
      } else {
        // value is object
        const value: Record<string, Json> = {};
        while (true) {
          const childResult = buildJson(events, index, childPath);
          value[childPath.key] = childResult.value;
          index = childResult.newIndex;
          if (index >= length) {
            break;
          }
          event = events[index];
          childPath = findDirectChildPath(path, event.path);
          if (!childPath || !(typeof childPath.key === "string")) {
            break;
          }
        }

        return {
          value,
          newIndex: index,
        };
      }
    }
  }

  assert(false, "invalid events");
}

export function buildJsonFromEvents(events: JsonEvent[]): Json {
  return buildJson(events, 0, undefined).value;
}
