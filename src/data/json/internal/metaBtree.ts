import {
  findAllBtreeEntriesWithPrefix,
  findFirstBtreeEntryWithPrefix,
  findLastBtreeEntry,
  insertBtreeEntry,
} from "../../btree/btree";
import { PageProvider, PageProviderForWrite } from "../../btree/pageProvider";
import { assert } from "../../misc/assert";
import { murmurHash3_x86_32 } from "../../misc/murmurHash3";
import { readTuple, tupleToUint8Array } from "../../uint8-array/tuple";
import { uint8ArraysEqual } from "../../uint8-array/uint8ArraysEqual";
import { JsonEventType, JsonPath } from "../jsonEvents";

/**
 * TODO
 */

// TODO add a cache map...

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

function buildBytesForPath(parentPathNumber: number, key: string | null): Uint8Array {
  return tupleToUint8Array(UINT32_STRING_TUPLE, [(parentPathNumber << 2) | (key === null ? 0b10 : 0), key || ""]);
}

function parseBytesForPath(bytes: Uint8Array): { parentPathNumber: number; key: string | null } {
  const [parentPathNumberAndExtra, key] = readTuple(bytes, UINT32_STRING_TUPLE).values;
  assert((parentPathNumberAndExtra & 1) === 0, "not a path entry");
  const isNull = (parentPathNumberAndExtra & 0b10) !== 0;
  const parentPathNumber = parentPathNumberAndExtra >>> 2;
  return {
    parentPathNumber,
    key: isNull ? null : key,
  };
}

function buildBytesForType(pathNumber: number, type: JsonEventType): Uint8Array {
  return tupleToUint8Array(UINT32_UINT32_TUPLE, [(pathNumber << 1) | 1, type]);
}

function parseBytesForType(bytes: Uint8Array): { pathNumber: number; type: JsonEventType } {
  const [pathNumberAndExtra, type] = readTuple(bytes, UINT32_UINT32_TUPLE).values;
  assert((pathNumberAndExtra & 1) === 1, "not a type entry");
  const pathNumber = pathNumberAndExtra >>> 1;
  return {
    pathNumber,
    type: type as JsonEventType,
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

export type JsonPathAndTypeToNumberCache = Map<JsonPath | undefined, number[]>;

function jsonPathToNumber(
  pageProvider: PageProviderForWrite,
  metaRootPageNumber: number,
  path: JsonPath | undefined,
  type: JsonEventType | undefined,
  cache?: JsonPathAndTypeToNumberCache
): number {
  const cacheIndex = type === undefined ? 0 : type + 1;
  const cachedResult = cache?.get(path)?.[cacheIndex];
  if (cachedResult !== undefined) {
    return cachedResult;
  }

  let result: number;
  if (type === undefined) {
    // just a path
    if (path === undefined) {
      // this is just 0
      return 0;
    }
    const parentPathNumber = jsonPathToNumber(pageProvider, metaRootPageNumber, path.parent, undefined);
    result = findOrCreateNumberForBytes(
      pageProvider,
      metaRootPageNumber,
      buildBytesForPath(parentPathNumber, path.key)
    );
  } else {
    const pathNumber = jsonPathToNumber(pageProvider, metaRootPageNumber, path, undefined);
    result = findOrCreateNumberForBytes(pageProvider, metaRootPageNumber, buildBytesForType(pathNumber, type));
  }

  if (cache) {
    let cacheArray = cache.get(path);
    if (!cacheArray) {
      cache.set(path, (cacheArray = []));
    }
    cacheArray[cacheIndex] = result;
  }
  return result;
}

export function jsonPathAndTypeToNumber(
  pageProvider: PageProviderForWrite,
  metaRootPageNumber: number,
  path: JsonPath | undefined,
  type: JsonEventType,
  cache?: JsonPathAndTypeToNumberCache
): number {
  return jsonPathToNumber(pageProvider, metaRootPageNumber, path, type, cache);
}

export type NumberToJsonPathAndTypeCache = Map<number, any>;

function numberToJsonPath(
  pageProvider: PageProvider,
  metaRootPageNumber: number,
  number: number,
  cache?: NumberToJsonPathAndTypeCache
): JsonPath | undefined | false {
  if (number === 0) {
    return undefined;
  }
  const cachedResult = cache?.get(number);
  if (cachedResult !== undefined) {
    return cachedResult;
  }
  const bytes = readBytesForNumber(pageProvider, metaRootPageNumber, number);
  if (bytes === false) {
    return false;
  }
  const { parentPathNumber, key } = parseBytesForPath(bytes);
  const parentPath = numberToJsonPath(pageProvider, metaRootPageNumber, parentPathNumber, cache);
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

export function numberToJsonPathAndType(
  pageProvider: PageProvider,
  metaRootPageNumber: number,
  number: number,
  cache?: NumberToJsonPathAndTypeCache
): { readonly path?: JsonPath; readonly type: JsonEventType } | false {
  const cachedResult = cache?.get(number);
  if (cachedResult !== undefined) {
    return cachedResult;
  }
  const bytes = readBytesForNumber(pageProvider, metaRootPageNumber, number);
  if (bytes === false) {
    return false;
  }
  const { pathNumber, type } = parseBytesForType(bytes);
  const path = numberToJsonPath(pageProvider, metaRootPageNumber, pathNumber, cache);
  if (path === false) {
    return false;
  }

  const result = { path, type };
  cache?.set(number, result);
  return result;
}
