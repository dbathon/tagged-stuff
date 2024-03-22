import {
  findAllBtreeEntriesWithPrefix,
  findFirstBtreeEntryWithPrefix,
  findLastBtreeEntry,
  insertBtreeEntry,
} from "../../btree/btree";
import { type PageProvider, type PageProviderForWrite } from "../../btree/pageProvider";
import { assert, murmurHash3_x86_32, readTuple, tupleToUint8Array, uint8ArraysEqual } from "shared-util";
import { type JsonPath, type JsonPathKey } from "../jsonEvents";

/**
 * TODO
 */

const LOOKUP_PREFIX = 0;
// put the actual entries after the lookup entries to be able to use findLastBtreeEntry()
const ENTRY_PREFIX = 1;

const UINT32_TUPLE = ["uint32"] as const;
const ARRAY_TUPLE = ["array"] as const;
const UINT32_STRING_TUPLE = ["uint32", "string"] as const;
const UINT32_UINT32_TUPLE = ["uint32", "uint32"] as const;
const UINT32_UINT32_ARRAY_TUPLE = ["uint32", "uint32", "array"] as const;
const UINT32_UINT32RAW_TUPLE = ["uint32", "uint32raw"] as const;
const UINT32_UINT32RAW_UINT32_TUPLE = ["uint32", "uint32raw", "uint32"] as const;

function buildBytesForPath(parentPathNumber: number, key: JsonPathKey): Uint8Array {
  return tupleToUint8Array(UINT32_STRING_TUPLE, [(parentPathNumber << 1) | (key === 0 ? 1 : 0), key || ""]);
}

function parseBytesForPath(bytes: Uint8Array): { parentPathNumber: number; key: JsonPathKey } {
  const [parentPathNumberAndExtra, key] = readTuple(bytes, UINT32_STRING_TUPLE).values;
  const isArray = (parentPathNumberAndExtra & 1) !== 0;
  const parentPathNumber = parentPathNumberAndExtra >>> 1;
  return {
    parentPathNumber,
    key: isArray ? 0 : key,
  };
}

function notFalse<T>(value: T | false): T {
  assert(value !== false);
  return value;
}

function readBytesForNumber(
  pageProvider: PageProvider,
  metaRootPageNumber: number,
  number: number
): Uint8Array | false {
  const entryPrefix = tupleToUint8Array(UINT32_UINT32_TUPLE, [ENTRY_PREFIX, number]);
  const findResult = findFirstBtreeEntryWithPrefix(pageProvider, metaRootPageNumber, entryPrefix);
  assert(findResult !== undefined, "no entry found for " + number);
  return findResult && readTuple(findResult, ARRAY_TUPLE, entryPrefix.length).values[0];
}

function findOrCreateNumberForBytes(
  pageProvider: PageProviderForWrite,
  metaRootPageNumber: number,
  bytes: Uint8Array
): number {
  const hash = murmurHash3_x86_32(bytes);
  const lookupPrefix = tupleToUint8Array(UINT32_UINT32RAW_TUPLE, [LOOKUP_PREFIX, hash]);
  for (const entry of notFalse(findAllBtreeEntriesWithPrefix(pageProvider.getPage, metaRootPageNumber, lookupPrefix))) {
    const number = readTuple(entry, UINT32_TUPLE, lookupPrefix.length).values[0];
    const entryBytes = notFalse(readBytesForNumber(pageProvider.getPage, metaRootPageNumber, number));
    // check that the entry actually matches
    if (uint8ArraysEqual(entryBytes, bytes)) {
      return number;
    }
  }

  // no existing entry found, create a new one
  const lastEntry = notFalse(findLastBtreeEntry(pageProvider.getPage, metaRootPageNumber));
  let nextNumber = 1;
  if (lastEntry) {
    const values = readTuple(lastEntry, UINT32_UINT32_TUPLE).values;
    assert(values[0] === ENTRY_PREFIX);
    nextNumber = values[1] + 1;
  }
  const newLookup = tupleToUint8Array(UINT32_UINT32RAW_UINT32_TUPLE, [LOOKUP_PREFIX, hash, nextNumber]);
  const newEntry = tupleToUint8Array(UINT32_UINT32_ARRAY_TUPLE, [ENTRY_PREFIX, nextNumber, bytes]);
  const lookupSuccess = insertBtreeEntry(pageProvider, metaRootPageNumber, newLookup);
  const entrySuccess = insertBtreeEntry(pageProvider, metaRootPageNumber, newEntry);
  assert(lookupSuccess && entrySuccess);
  return nextNumber;
}

export type JsonPathToNumberCache = Map<JsonPath, number>;

const NUMBER_FOR_EMPTY_PARENT = 0;

export function jsonPathToNumber(
  pageProvider: PageProviderForWrite,
  metaRootPageNumber: number,
  path: JsonPath,
  cache?: JsonPathToNumberCache
): number {
  const cachedResult = cache?.get(path);
  if (cachedResult !== undefined) {
    return cachedResult;
  }

  const parentPathNumber = path.parent
    ? jsonPathToNumber(pageProvider, metaRootPageNumber, path.parent, undefined)
    : NUMBER_FOR_EMPTY_PARENT;
  const result = findOrCreateNumberForBytes(
    pageProvider,
    metaRootPageNumber,
    buildBytesForPath(parentPathNumber, path.key)
  );
  assert(result > NUMBER_FOR_EMPTY_PARENT, "path numbers need to be greater than 0");

  cache?.set(path, result);
  return result;
}

export type NumberToJsonPathCache = Map<number, JsonPath>;

export function numberToJsonPath(
  pageProvider: PageProvider,
  metaRootPageNumber: number,
  number: number,
  cache?: NumberToJsonPathCache
): JsonPath | false {
  const cachedResult = cache?.get(number);
  if (cachedResult !== undefined) {
    return cachedResult;
  }
  const bytes = readBytesForNumber(pageProvider, metaRootPageNumber, number);
  if (bytes === false) {
    return false;
  }
  const { parentPathNumber, key } = parseBytesForPath(bytes);
  const parentPath =
    parentPathNumber === NUMBER_FOR_EMPTY_PARENT
      ? undefined
      : numberToJsonPath(pageProvider, metaRootPageNumber, parentPathNumber, cache);
  if (parentPath === false) {
    return false;
  }
  const result = {
    parent: parentPath,
    key,
  };
  cache?.set(number, result);
  return result;
}
