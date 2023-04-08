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
import { PageProviderForWrite } from "../btree/pageProvider";
import { PageAccessDuringTransaction } from "../page-store/PageAccessDuringTransaction";
import { PageData } from "../page-store/PageData";
import { getTupleByteLength, readTuple, tupleToUint8Array, writeTuple } from "../uint8-array/tuple";
import { jsonPathAndTypeToNumber, numberToJsonPathAndType } from "./internal/metaBtree";
import {
  buildJsonFromEvents,
  JsonEvent,
  JsonEventType,
  JsonPath,
  JSON_NUMBER,
  JSON_STRING,
  produceJsonEvents,
} from "./jsonEvents";

/** Some magic number to mark a page store as a json store. */
const MAGIC_NUMBER = 1983760274;

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
const UINT32_UINT32_UINT32_TUPLE = ["uint32", "uint32", "uint32"] as const;
const STRING_UINT32_TUPLE = ["string", "uint32"] as const;
const STRING_UINT32_UINT32_UINT32_UINT32_TUPLE = ["string", "uint32", "uint32", "uint32", "uint32"] as const;
const UINT32_UINT32_STRING_TUPLE = ["uint32", "uint32", "string"] as const;
const UINT32_UINT32_NUMBER_TUPLE = ["uint32", "uint32", "number"] as const;
const STRING_TUPLE = ["string"] as const;
const NUMBER_TUPLE = ["number"] as const;

/** Internal "exception object" indicating missing pages, all exported functions must catch and handle it. */
const MISSING_PAGE = {};

export type PageAccess = (pageNumber: number) => PageData | undefined;

type PageAccessNotUndefined = (pageNumber: number) => PageData;

type PageProviderNotUndefined = (pageNumber: number) => Uint8Array;

function notFalse<T>(value: T | false): T {
  if (value === false) {
    throw new Error("unexpected");
  }
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
      const freePageEntry = findFirstBtreeEntry(provider.getPage, FREE_LIST_ROOT_PAGE_NUMBER);
      // if we are in releasePage, then don't try to allocate from the free list
      if (!inReleasePage && freePageEntry) {
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
      } else {
        const zeroPage = pageAccess.getForUpdate(0).dataView;
        const nextPageNumber = zeroPage.getUint32(MAX_ALLOCATED_OFFSET) + 1;
        zeroPage.setUint32(MAX_ALLOCATED_OFFSET, nextPageNumber);
        return nextPageNumber;
      }
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
  if (magic !== MAGIC_NUMBER) {
    throw new Error("page store is not a json store");
  }
  return true;
}

interface TableInfo {
  /** Contains the entries. */
  mainRoot: number;
  /** Contains the used json paths/types. */
  metaRoot: number;
  /** Contains the string and number values of the entries. */
  attributeRoot: number;
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
  const [mainRoot, metaRoot, attributeRoot] = readTuple(tableEntry, UINT32_UINT32_UINT32_TUPLE, prefix.length).values;
  return { mainRoot, metaRoot, attributeRoot };
}

function getOrCreateTableInfo(pageProvider: PageProviderForWrite, tableName: string): TableInfo {
  const existingTableInfo = getTableInfo(pageProvider.getPage, tableName);
  if (existingTableInfo) {
    return existingTableInfo;
  }
  const mainRoot = allocateAndInitBtreeRootPage(pageProvider);
  const metaRoot = allocateAndInitBtreeRootPage(pageProvider);
  const attributeRoot = allocateAndInitBtreeRootPage(pageProvider);

  notFalse(
    insertBtreeEntry(
      pageProvider,
      TABLES_ROOT_PAGE_NUMBER,
      tupleToUint8Array(STRING_UINT32_UINT32_UINT32_UINT32_TUPLE, [tableName, 0, mainRoot, metaRoot, attributeRoot])
    )
  );

  return { mainRoot, metaRoot, attributeRoot };
}

interface HasId {
  id?: number;
}

/**
 * Work in progress...
 */
interface QueryParameters {
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

    return entries.map((entry) => {
      const idResult = readTuple(entry, UINT32_TUPLE, 0);
      const id = idResult.values[0];
      let entryOffset = idResult.length;

      const attributeEntries = new Map<number, Uint8Array>();
      const attributePrefix = entry.subarray(0, idResult.length);
      for (const attributeEntry of notFalse(
        findAllBtreeEntriesWithPrefix(pageProvider, tableInfo.attributeRoot, attributePrefix)
      )) {
        const indexResult = readTuple(attributeEntry, UINT32_TUPLE, idResult.length);
        attributeEntries.set(indexResult.values[0], attributeEntry.subarray(idResult.length + indexResult.length));
      }

      let eventIndex = 0;
      const events: JsonEvent[] = [];
      while (entryOffset < entry.length) {
        const eventResult = readTuple(entry, UINT32_TUPLE, entryOffset);
        const eventNumber = eventResult.values[0];
        const { type, path } = notFalse(numberToJsonPathAndType(pageProvider, tableInfo.metaRoot, eventNumber));
        let value: string | number | undefined = undefined;
        if (type === JSON_STRING || type === JSON_NUMBER) {
          const attributeValueArray = attributeEntries.get(eventIndex);
          if (!attributeValueArray) {
            throw new Error("attribute entry missing");
          }
          if (type === JSON_STRING) {
            value = readTuple(attributeValueArray, STRING_TUPLE).values[0];
          } else {
            value = readTuple(attributeValueArray, NUMBER_TUPLE).values[0];
          }
        }
        events.push({ path, type, value });
        entryOffset += eventResult.length;
        eventIndex++;
      }
      notFalse(entryOffset === entry.length);

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
    zeroPage.setUint32(MAGIC_NUMBER_OFFSET, MAGIC_NUMBER);

    let maxAllocated = 0;
    const initPageProvider: PageProviderForWrite = {
      getPage: (pageNumber: number) => pageAccess.get(pageNumber).array,
      getPageForUpdate: (pageNumber: number) => pageAccess.getForUpdate(pageNumber).array,
      allocateNewPage: () => ++maxAllocated,
      releasePage: (pageNumber: number) => {
        throw new Error("unexpected");
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

    // also delete entries in attributeRoot
    for (const attributeEntry of notFalse(
      findAllBtreeEntriesWithPrefix(pageProvider.getPage, tableInfo.attributeRoot, prefix)
    )) {
      notFalse(removeBtreeEntry(pageProvider, tableInfo.attributeRoot, attributeEntry));
    }
    return true;
  } catch (e) {
    if (e === MISSING_PAGE) {
      throw new Error("unexpected");
    } else {
      throw e;
    }
  }
}

function filterId(key: string, parentPath: JsonPath | undefined): boolean {
  if (!parentPath && key === "id") {
    return false;
  }
  return true;
}

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

    let entryLength = getTupleByteLength(UINT32_TUPLE, [id]);
    const eventNumbers: number[] = [];
    produceJsonEvents(
      json,
      (type: JsonEventType, path: JsonPath | undefined, value?: string | number) => {
        const eventNumber = jsonPathAndTypeToNumber(pageProvider, tableInfo.metaRoot, path, type);
        if (value !== undefined) {
          const attributeIndex = eventNumbers.length;
          let attributeEntry: Uint8Array;
          if (typeof value === "string") {
            // TODO: split longer strings into multiple entries?!
            attributeEntry = tupleToUint8Array(UINT32_UINT32_STRING_TUPLE, [id!, attributeIndex, value]);
          } else {
            attributeEntry = tupleToUint8Array(UINT32_UINT32_NUMBER_TUPLE, [id!, attributeIndex, value]);
          }
          notFalse(insertBtreeEntry(pageProvider, tableInfo.attributeRoot, attributeEntry));
        }
        entryLength += getTupleByteLength(UINT32_TUPLE, [eventNumber]);
        eventNumbers.push(eventNumber);
      },
      filterId
    );

    const entry = new Uint8Array(entryLength);
    let entryOffset = writeTuple(entry, 0, UINT32_TUPLE, [id]);
    for (const eventNumber of eventNumbers) {
      entryOffset += writeTuple(entry, entryOffset, UINT32_TUPLE, [eventNumber]);
    }
    notFalse(entryOffset === entry.length);

    notFalse(insertBtreeEntry(pageProvider, tableInfo.mainRoot, entry));

    if (newEntry) {
      (json as HasId).id = id;
    }
  } catch (e) {
    if (e === MISSING_PAGE) {
      throw new Error("unexpected");
    } else {
      throw e;
    }
  }
}
