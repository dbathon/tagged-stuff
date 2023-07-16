import {
  allocateAndInitBtreeRootPage,
  findAllBtreeEntriesWithPrefix,
  findFirstBtreeEntry,
  findFirstBtreeEntryWithPrefix,
  findLastBtreeEntry,
  insertBtreeEntry,
  removeBtreeEntry,
  scanBtreeEntries,
} from "../btree/btree";
import { PageProvider, PageProviderForWrite } from "../btree/pageProvider";
import { assert } from "../misc/assert";
import { PageAccessDuringTransaction } from "../page-store/PageAccessDuringTransaction";
import { PageData } from "../page-store/PageData";
import { getTupleByteLength, readTuple, tupleToUint8Array, writeTuple } from "../uint8-array/tuple";
import { JsonPathToNumberCache, NumberToJsonPathCache, jsonPathToNumber, numberToJsonPath } from "./internal/metaBtree";
import { deserializeJsonEvents, serializeJsonEvents } from "./internal/serializeJsonEvents";
import { buildJsonFromEvents, JsonEvent, JsonEventType, JsonPath, produceJsonEvents } from "./jsonEvents";

/** Some magic number to mark a page store as a json store. */
const MAGIC_NUMBER_V1 = 1983760274;

const FREE_LIST_ROOT_PAGE_NUMBER = 1;
const TABLES_ROOT_PAGE_NUMBER = 2;

/**
 * 0 page has (all uint32 uncompressed):
 *  magic number
 *  max allocated page
 */

// offsets of values on the zero page
const MAGIC_NUMBER_OFFSET = 0 << 2;
const MAX_ALLOCATED_OFFSET = 1 << 2;

const UINT32_TUPLE = ["uint32"] as const;
const UINT32_UINT32_TUPLE = ["uint32", "uint32"] as const;
const UINT32_UINT32_UINT32_UINT32_TUPLE = ["uint32", "uint32", "uint32", "uint32"] as const;
const STRING_UINT32_TUPLE = ["string", "uint32"] as const;
const STRING_UINT32_UINT32_UINT32_UINT32_UINT32_TUPLE = [
  "string",
  "uint32",
  "uint32",
  "uint32",
  "uint32",
  "uint32",
] as const;

/** Internal "exception object" indicating missing pages, all exported functions must catch and handle it. */
const MISSING_PAGE = {};

export type PageAccess = (pageNumber: number) => PageData | undefined;

type PageAccessNotUndefined = (pageNumber: number) => PageData;

type PageProviderNotUndefined = (pageNumber: number) => Uint8Array;

function notFalse<T>(value: T | false): T {
  assert(value !== false);
  return value;
}

function toPageAccessNotUndefined(pageAccess: PageAccess): PageAccessNotUndefined {
  return (pageNumber) => {
    const result = pageAccess(pageNumber);
    if (!result) {
      throw MISSING_PAGE;
    }
    return result;
  };
}

function toPageProvider(pageAccess: PageAccessNotUndefined): PageProviderNotUndefined {
  return (pageNumber) => pageAccess(pageNumber).array;
}

/** Assumes that it is an initialized page store. */
function toPageProviderForWrite(pageAccess: PageAccessDuringTransaction): PageProviderForWrite {
  let tempReleases: number[] | undefined = undefined;
  let inReleasePage = false;
  const provider: PageProviderForWrite = {
    getPage(pageNumber) {
      return pageAccess.get(pageNumber).array;
    },
    getPageForUpdate(pageNumber) {
      return pageAccess.getForUpdate(pageNumber).array;
    },
    allocateNewPage() {
      // if we are in releasePage, then don't try to allocate from the free list
      if (!inReleasePage) {
        const freePageEntry = findFirstBtreeEntry(provider.getPage, FREE_LIST_ROOT_PAGE_NUMBER);
        if (freePageEntry) {
          const freePageNumber = readTuple(freePageEntry, UINT32_TUPLE, 0).values[0];
          const cleanupNeeded = !tempReleases;
          if (!tempReleases) {
            tempReleases = [];
          }
          notFalse(removeBtreeEntry(provider, FREE_LIST_ROOT_PAGE_NUMBER, freePageEntry));
          if (cleanupNeeded) {
            const pagesToRelease = tempReleases;
            tempReleases = undefined;
            for (const pageToRelease of pagesToRelease) {
              provider.releasePage(pageToRelease);
            }
          }
          return freePageNumber;
        }
      }

      const zeroPage = pageAccess.getForUpdate(0).dataView;
      const nextPageNumber = zeroPage.getUint32(MAX_ALLOCATED_OFFSET) + 1;
      zeroPage.setUint32(MAX_ALLOCATED_OFFSET, nextPageNumber);
      return nextPageNumber;
    },
    releasePage(pageNumber) {
      if (tempReleases) {
        tempReleases.push(pageNumber);
      } else {
        if (inReleasePage) {
          throw new Error("releasePage() cannot be called inside releasePage()");
        }
        inReleasePage = true;
        notFalse(insertBtreeEntry(provider, FREE_LIST_ROOT_PAGE_NUMBER, tupleToUint8Array(UINT32_TUPLE, [pageNumber])));
        inReleasePage = false;
      }
    },
  };
  return provider;
}

function isInitialized(pageAccess: PageAccessNotUndefined): boolean {
  const zeroPage = pageAccess(0).dataView;
  const magic = zeroPage.getUint32(MAGIC_NUMBER_OFFSET);
  if (magic === 0) {
    // assume it is an entirely uninitialized/unused page store...
    // TODO: maybe check that the whole page is 0
    return false;
  }
  if (magic !== MAGIC_NUMBER_V1) {
    throw new Error("page store is not a json store");
  }
  return true;
}

const TABLE_INFO_VERSION = 1;

interface TableInfo {
  /** TableInfo version number (currently always 1, see TABLE_INFO_VERSION) */
  version: number;
  /** Contains the entries. */
  mainRoot: number;
  /** Contains the used json paths/types. */
  metaRoot: number;
  /** Contains overflow entries for entries, that cannot be stored in one entry. */
  overflowRoot: number;
  // TODO indexRoot
}

/** Assumes that it is an initialized page store. */
function getTableInfo(pageProvider: PageProviderNotUndefined, tableName: string): TableInfo | undefined {
  const prefix = tupleToUint8Array(STRING_UINT32_TUPLE, [tableName, 0]);
  if (prefix.indexOf(0) !== prefix.length - 1) {
    // we use 0 as a separator, so 0 is not allowed in the table name
    throw new Error("invalid tableName");
  }
  const tableEntry = findFirstBtreeEntryWithPrefix(pageProvider, TABLES_ROOT_PAGE_NUMBER, prefix);
  if (!tableEntry) {
    return undefined;
  }
  const [version, mainRoot, metaRoot, overflowRoot] = readTuple(
    tableEntry,
    UINT32_UINT32_UINT32_UINT32_TUPLE,
    prefix.length
  ).values;
  assert(version === TABLE_INFO_VERSION, "unexpected TableInfo version");
  return { version, mainRoot, metaRoot, overflowRoot };
}

function getOrCreateTableInfo(pageProvider: PageProviderForWrite, tableName: string): TableInfo {
  const existingTableInfo = getTableInfo(pageProvider.getPage, tableName);
  if (existingTableInfo) {
    return existingTableInfo;
  }
  const version = TABLE_INFO_VERSION;
  const mainRoot = allocateAndInitBtreeRootPage(pageProvider);
  const metaRoot = allocateAndInitBtreeRootPage(pageProvider);
  const overflowRoot = allocateAndInitBtreeRootPage(pageProvider);

  notFalse(
    insertBtreeEntry(
      pageProvider,
      TABLES_ROOT_PAGE_NUMBER,
      tupleToUint8Array(STRING_UINT32_UINT32_UINT32_UINT32_UINT32_TUPLE, [
        tableName,
        0,
        version,
        mainRoot,
        metaRoot,
        overflowRoot,
      ])
    )
  );

  return { version, mainRoot, metaRoot, overflowRoot };
}

// JsonEventType is in the range of 0 to 7
const TYPE_BITS = 3;
const TYPE_MASK = (1 << TYPE_BITS) - 1;
const MAX_PATH_NUMBER = -1 >>> TYPE_BITS;

/**
 * Encode the type as the last 3 bits of the number and use all other bits for the path.
 */
function jsonPathAndTypeToNumber(
  pageProvider: PageProviderForWrite,
  metaRootPageNumber: number,
  path: JsonPath,
  type: JsonEventType,
  cache: JsonPathToNumberCache
): number {
  assert((type & TYPE_MASK) === type, "unexpected JsonEventType");
  const pathNumber = jsonPathToNumber(pageProvider, metaRootPageNumber, path, cache);
  assert(pathNumber <= MAX_PATH_NUMBER, "too many paths in table");
  return ((pathNumber << TYPE_BITS) | type) >>> 0;
}

/**
 * See jsonPathAndTypeToNumber().
 */
function numberToJsonPathAndType(
  pageProvider: PageProvider,
  metaRootPageNumber: number,
  number: number,
  cache: NumberToJsonPathCache
): { path: JsonPath; type: JsonEventType } | false {
  const pathNumber = number >>> TYPE_BITS;
  const type = (number & TYPE_MASK) as JsonEventType;

  const path = numberToJsonPath(pageProvider, metaRootPageNumber, pathNumber, cache);

  return path && { path, type };
}

interface HasId {
  id?: number;
}

/**
 * Work in progress...
 */
export interface QueryParameters {
  minId?: number;
  maxResults?: number;
}

// T can just be specified, it is not validated...
export function queryJson<T extends object | unknown = unknown>(
  pageAccess: PageAccess,
  tableName: string,
  queryParameters: QueryParameters = {}
): T[] | false {
  try {
    const pageAccessNotUndefined = toPageAccessNotUndefined(pageAccess);
    if (!isInitialized(pageAccessNotUndefined)) {
      // every table is empty ;)
      return [];
    }
    const pageProvider = toPageProvider(pageAccessNotUndefined);
    const tableInfo = getTableInfo(pageProvider, tableName);
    if (!tableInfo) {
      // the table does not exist, so it is empty
      return [];
    }

    const entries: Uint8Array[] = [];
    const { maxResults } = queryParameters;
    scanBtreeEntries(
      pageProvider,
      tableInfo.mainRoot,
      queryParameters.minId ? tupleToUint8Array(UINT32_TUPLE, [queryParameters.minId]) : undefined,
      (entry) => {
        entries.push(entry);
        return maxResults === undefined || entries.length < maxResults;
      }
    );

    const cache: NumberToJsonPathCache = new Map();
    return entries.map((entry) => {
      const idAndZeroOrLengthResult = readTuple(entry, UINT32_UINT32_TUPLE, 0);
      const [id, zeroOrLength] = idAndZeroOrLengthResult.values;

      let jsonEventsArray = entry.subarray(idAndZeroOrLengthResult.length);
      if (zeroOrLength > 0) {
        // there is overflow
        const parts = [jsonEventsArray];
        let totalLength = jsonEventsArray.length;
        const overFlowPrefix = tupleToUint8Array(UINT32_TUPLE, [id]);
        for (const overflowEntry of notFalse(
          findAllBtreeEntriesWithPrefix(pageProvider, tableInfo.overflowRoot, overFlowPrefix)
        )) {
          const part = overflowEntry.subarray(readTuple(overflowEntry, UINT32_UINT32_TUPLE).length);
          parts.push(part);
          totalLength += part.length;
        }

        // concatenate all the parts
        jsonEventsArray = new Uint8Array(totalLength);
        let offset = 0;
        for (const part of parts) {
          jsonEventsArray.set(part, offset);
          offset += part.length;
        }
      }

      const events = deserializeJsonEvents(jsonEventsArray, (eventNumber) =>
        notFalse(numberToJsonPathAndType(pageProvider, tableInfo.metaRoot, eventNumber, cache))
      );

      const result = buildJsonFromEvents(events);
      (result as HasId).id = id;

      return result as T;
    });
  } catch (e) {
    if (e === MISSING_PAGE) {
      return false;
    } else {
      throw e;
    }
  }
}

function initializeIfNecessary(pageAccess: PageAccessDuringTransaction): void {
  if (!isInitialized(pageAccess.get)) {
    const zeroPage = pageAccess.getForUpdate(0).dataView;
    zeroPage.setUint32(MAGIC_NUMBER_OFFSET, MAGIC_NUMBER_V1);

    let maxAllocated = 0;
    const initPageProvider: PageProviderForWrite = {
      getPage: (pageNumber: number) => pageAccess.get(pageNumber).array,
      getPageForUpdate: (pageNumber: number) => pageAccess.getForUpdate(pageNumber).array,
      allocateNewPage: () => ++maxAllocated,
      releasePage: (pageNumber: number) => {
        assert(false);
      },
    };

    const rootPageOne = allocateAndInitBtreeRootPage(initPageProvider);
    const rootPageTwo = allocateAndInitBtreeRootPage(initPageProvider);

    if (rootPageOne !== FREE_LIST_ROOT_PAGE_NUMBER || rootPageTwo !== TABLES_ROOT_PAGE_NUMBER || maxAllocated !== 2) {
      throw new Error("init failed");
    }

    zeroPage.setUint32(MAX_ALLOCATED_OFFSET, maxAllocated);
  }
}

export function deleteJson(pageAccess: PageAccessDuringTransaction, tableName: string, id: number): boolean {
  try {
    if (!isInitialized(pageAccess.get)) {
      // nothing to delete
      return false;
    }

    const pageProvider = toPageProviderForWrite(pageAccess);
    const tableInfo = getOrCreateTableInfo(pageProvider, tableName);

    const prefix = tupleToUint8Array(UINT32_TUPLE, [id]);
    const mainEntry = findFirstBtreeEntryWithPrefix(pageProvider.getPage, tableInfo.mainRoot, prefix);
    if (!mainEntry) {
      // does not exist
      return false;
    }
    notFalse(removeBtreeEntry(pageProvider, tableInfo.mainRoot, mainEntry));

    const zeroOrLength = readTuple(mainEntry, UINT32_UINT32_TUPLE).values[1];
    if (zeroOrLength > 0) {
      // also delete entries in overflowRoot
      while (true) {
        const overflowEntry = findFirstBtreeEntryWithPrefix(pageProvider.getPage, tableInfo.overflowRoot, prefix);
        if (!overflowEntry) {
          break;
        }
        notFalse(removeBtreeEntry(pageProvider, tableInfo.overflowRoot, overflowEntry));
      }
    }

    return true;
  } catch (e) {
    assert(e !== MISSING_PAGE);
    throw e;
  }
}

function filterId(key: string, parentPath: JsonPath | undefined): boolean {
  if (!parentPath && key === "id") {
    return false;
  }
  return true;
}

/**
 * It is unclear what the best value for this would be, but since it is only used for writing, we can just use some
 * value for now and potentially find a better one later (or even determine the best one dynamically)...
 */
const MAX_SINGLE_PAGE_JSON_EVENTS_ARRAY_LENGTH = 1000;

// modifies the given json by adding an id if it does not have one already...
export function saveJson(pageAccess: PageAccessDuringTransaction, tableName: string, json: object): void {
  if (!json || Array.isArray(json)) {
    throw new TypeError("json needs to be an object");
  }
  try {
    initializeIfNecessary(pageAccess);

    const pageProvider = toPageProviderForWrite(pageAccess);
    const tableInfo = getOrCreateTableInfo(pageProvider, tableName);

    let id = (json as HasId).id;
    const newEntry = id === undefined;
    if (newEntry) {
      const lastEntry = notFalse(findLastBtreeEntry(pageProvider.getPage, tableInfo.mainRoot));
      id = lastEntry ? readTuple(lastEntry, UINT32_TUPLE, 0).values[0] + 1 : 0;
    } else {
      if (!(typeof id === "number")) {
        throw new Error("id is not a number");
      }
      // for now just delete everything and insert again...
      // TODO: optimize this to only do necessary changes
      if (!deleteJson(pageAccess, tableName, id)) {
        throw new Error("json with id does not exist");
      }
    }

    const jsonEvents: JsonEvent[] = [];
    produceJsonEvents(
      json,
      (type, path, value) => {
        jsonEvents.push({ path, type, value });
      },
      filterId
    );

    const cache: JsonPathToNumberCache = new Map();
    const jsonEventsArray = serializeJsonEvents(jsonEvents, (path, type) => {
      // json is an object so path can never be undefined
      assert(path !== undefined);
      return jsonPathAndTypeToNumber(pageProvider, tableInfo.metaRoot, path, type, cache);
    });

    let zeroOrLength: number;
    let firstJsonEventsArrayPart: Uint8Array;
    if (jsonEventsArray.length <= MAX_SINGLE_PAGE_JSON_EVENTS_ARRAY_LENGTH) {
      // in this case always use 0, which then just means "no overflow"
      zeroOrLength = 0;
      firstJsonEventsArrayPart = jsonEventsArray;
    } else {
      // split the array and write the other parts into the overflow
      zeroOrLength = jsonEventsArray.length;
      firstJsonEventsArrayPart = jsonEventsArray.subarray(0, MAX_SINGLE_PAGE_JSON_EVENTS_ARRAY_LENGTH);
      for (
        let offset = MAX_SINGLE_PAGE_JSON_EVENTS_ARRAY_LENGTH, i = 0;
        offset < zeroOrLength;
        offset += MAX_SINGLE_PAGE_JSON_EVENTS_ARRAY_LENGTH, i++
      ) {
        const overflowArray = jsonEventsArray.subarray(
          offset,
          Math.min(offset + MAX_SINGLE_PAGE_JSON_EVENTS_ARRAY_LENGTH, zeroOrLength)
        );
        const idAndIndex = [id, i] as const;
        const overflowEntryLength = getTupleByteLength(UINT32_UINT32_TUPLE, idAndIndex) + overflowArray.length;
        const overflowEntry = new Uint8Array(overflowEntryLength);
        const overflowArrayOffset = writeTuple(overflowEntry, 0, UINT32_UINT32_TUPLE, idAndIndex);
        overflowEntry.set(overflowArray, overflowArrayOffset);
        assert(insertBtreeEntry(pageProvider, tableInfo.overflowRoot, overflowEntry));
      }
    }

    const idAndZeroOrLength = [id, zeroOrLength] as const;
    const entryLength = getTupleByteLength(UINT32_UINT32_TUPLE, idAndZeroOrLength) + firstJsonEventsArrayPart.length;
    const entry = new Uint8Array(entryLength);
    const entryOffset = writeTuple(entry, 0, UINT32_UINT32_TUPLE, idAndZeroOrLength);
    entry.set(firstJsonEventsArrayPart, entryOffset);

    assert(insertBtreeEntry(pageProvider, tableInfo.mainRoot, entry));

    if (newEntry) {
      (json as HasId).id = id;
    }
  } catch (e) {
    assert(e !== MISSING_PAGE);
    throw e;
  }
}
