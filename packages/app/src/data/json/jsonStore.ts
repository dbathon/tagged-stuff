import {
  allocateAndInitBtreeRootPage,
  findAllBtreeEntriesWithPrefix,
  findFirstBtreeEntry,
  findFirstBtreeEntryWithPrefix,
  findLastBtreeEntry,
  insertBtreeEntry,
  removeBtreeEntry,
  scanBtreeEntries,
} from "btree";
import { type PageProviderForWrite } from "btree";
import { type PageAccessDuringTransaction } from "page-store";
import {
  assert,
  Uint8ArraySet,
  getTupleByteLength,
  readTuple,
  tupleToUint8Array,
  writeTuple,
  uint8ArrayToDataView,
} from "shared-util";
import { compareJsonPrimitives } from "./internal/compareJsonPrimitives";
import { buildIndexEntries, updateIndexForJson } from "./internal/indexing";
import {
  type JsonPathToNumberCache,
  type NumberToJsonPathCache,
  jsonPathToNumber,
  numberToJsonPath,
} from "./internal/metaBtree";
import { deserializeJsonEvents, serializeJsonEvents } from "./internal/serializeJsonEvents";
import {
  buildJsonFromEvents,
  type FullJsonEvent,
  JSON_EMPTY_OBJECT,
  type JsonPath,
  produceJsonEvents,
} from "./jsonEvents";
import {
  type CountParameters,
  type FilterCondition,
  type JsonPrimitive,
  type Operator,
  type Path,
  type PathArray,
  type ProjectionType,
  type QueryParameters,
  type QueryResult,
} from "./queryTypes";

/** Some magic number to mark a page store as a json store. */
const MAGIC_NUMBER_V1 = 1983760274;

/** Page 0 is just used for the "magic number" */
const MAGIC_NUMBER_PAGE_NUMBER = 0;
/**
 * Page 1 used for the "max allocated page number". This is separate from page 0 to avoid allocating a new page
 * triggers "all" queries to re-run (because they will all read page 0).
 */
const MAX_ALLOCATED_PAGE_NUMBER = 1;
const FREE_LIST_ROOT_PAGE_NUMBER = 2;
const TABLES_ROOT_PAGE_NUMBER = 3;

const UINT32_TUPLE = ["uint32"] as const;
const UINT32_UINT32_TUPLE = ["uint32", "uint32"] as const;
const UINT32_UINT32_UINT32_UINT32_UINT32_TUPLE = ["uint32", "uint32", "uint32", "uint32", "uint32"] as const;
const STRING_UINT32_TUPLE = ["string", "uint32"] as const;
const STRING_UINT32_UINT32_UINT32_UINT32_UINT32_UINT32_TUPLE = [
  "string",
  "uint32",
  "uint32",
  "uint32",
  "uint32",
  "uint32",
  "uint32",
] as const;

/** Internal "exception object" indicating missing pages, all exported functions must catch and handle it. */
const MISSING_PAGE = {};

export type PageAccess = (pageNumber: number) => Uint8Array | undefined;

export type PageAccessNotUndefined = (pageNumber: number) => Uint8Array;

type PageProviderNotUndefined = PageAccessNotUndefined;

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

/** Assumes that it is an initialized page store. */
function toPageProviderForWrite(pageAccess: PageAccessDuringTransaction): PageProviderForWrite {
  let tempReleases: number[] | undefined = undefined;
  let inReleasePage = false;
  const provider: PageProviderForWrite = {
    getPage(pageNumber) {
      return pageAccess.get(pageNumber);
    },
    getPageForUpdate(pageNumber) {
      return pageAccess.getForUpdate(pageNumber);
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

      const maxAllocatedPage = uint8ArrayToDataView(pageAccess.getForUpdate(MAX_ALLOCATED_PAGE_NUMBER));
      const nextPageNumber = maxAllocatedPage.getUint32(0) + 1;
      maxAllocatedPage.setUint32(0, nextPageNumber);
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
  const zeroPage = pageAccess(MAGIC_NUMBER_PAGE_NUMBER);
  const magic = ((zeroPage[0] << 24) | (zeroPage[1] << 16) | (zeroPage[2] << 8) | zeroPage[3]) >>> 0;
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
  /** Contains the index entries. */
  indexRoot: number;
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
  const [version, mainRoot, metaRoot, overflowRoot, indexRoot] = readTuple(
    tableEntry,
    UINT32_UINT32_UINT32_UINT32_UINT32_TUPLE,
    prefix.length,
  ).values;
  assert(version === TABLE_INFO_VERSION, "unexpected TableInfo version");
  return { version, mainRoot, metaRoot, overflowRoot, indexRoot };
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
  const indexRoot = allocateAndInitBtreeRootPage(pageProvider);

  notFalse(
    insertBtreeEntry(
      pageProvider,
      TABLES_ROOT_PAGE_NUMBER,
      tupleToUint8Array(STRING_UINT32_UINT32_UINT32_UINT32_UINT32_UINT32_TUPLE, [
        tableName,
        0,
        version,
        mainRoot,
        metaRoot,
        overflowRoot,
        indexRoot,
      ]),
    ),
  );

  return { version, mainRoot, metaRoot, overflowRoot, indexRoot };
}

function readEntryIdAndJsonEvents(
  entry: Uint8Array,
  tableInfo: TableInfo,
  pathNumberToPath: (pathNumber: number | undefined) => JsonPath | undefined,
  pageProvider: PageProviderNotUndefined,
): { id: number; events: FullJsonEvent[] } {
  const idAndZeroOrLengthResult = readTuple(entry, UINT32_UINT32_TUPLE, 0);
  const [id, zeroOrLength] = idAndZeroOrLengthResult.values;

  let jsonEventsArray = entry.subarray(idAndZeroOrLengthResult.length);
  if (zeroOrLength > 0) {
    // there is overflow
    const parts = [jsonEventsArray];
    let totalLength = jsonEventsArray.length;
    const overFlowPrefix = tupleToUint8Array(UINT32_TUPLE, [id]);
    for (const overflowEntry of notFalse(
      findAllBtreeEntriesWithPrefix(pageProvider, tableInfo.overflowRoot, overFlowPrefix),
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

  return {
    id,
    events: deserializeJsonEvents(jsonEventsArray, (pathNumber, type, value) => ({
      pathNumber,
      path: pathNumberToPath(pathNumber),
      type,
      value,
    })),
  };
}

function createPathNumberToPath(
  tableInfo: TableInfo,
  pageProvider: PageProviderNotUndefined,
): (pathNumber: number | undefined) => JsonPath | undefined {
  const cache: NumberToJsonPathCache = new Map();
  return (pathNumber) => {
    if (pathNumber === undefined) {
      return undefined;
    }
    return notFalse(numberToJsonPath(pageProvider, tableInfo.metaRoot, pathNumber, cache));
  };
}

interface HasId {
  id?: number;
}

function readAllEntries<T extends object | unknown = unknown>(pageAccess: PageAccess, tableName: string): T[] | false {
  try {
    const pageAccessNotUndefined = toPageAccessNotUndefined(pageAccess);
    if (!isInitialized(pageAccessNotUndefined)) {
      // every table is empty ;)
      return [];
    }
    const pageProvider = pageAccessNotUndefined;
    const tableInfo = getTableInfo(pageProvider, tableName);
    if (!tableInfo) {
      // the table does not exist, so it is empty
      return [];
    }

    const entries: Uint8Array[] = [];
    scanBtreeEntries(pageProvider, tableInfo.mainRoot, undefined, (entry) => {
      entries.push(entry);
      return true;
    });

    const pathNumberToPath = createPathNumberToPath(tableInfo, pageProvider);
    return entries.map((entry) => {
      const { id, events } = readEntryIdAndJsonEvents(entry, tableInfo, pathNumberToPath, pageProvider);

      let result: unknown;
      if (events.length) {
        result = buildJsonFromEvents(events);
      } else {
        // special case, just an empty object, see emptyObjectWithoutPathCount in saveJson()
        result = {};
      }
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

function pathToPathArray(path: Path): PathArray {
  if (typeof path === "string") {
    const parts = path.split(".");
    if (parts.length === 1 && !parts[0].endsWith("[]")) {
      // fast path
      return parts as PathArray;
    }
    const result: (string | 0)[] = [];
    for (const part of parts) {
      let propertyName = part;
      let arrayAccessCount = 0;
      while (propertyName.endsWith("[]")) {
        ++arrayAccessCount;
        propertyName = propertyName.substring(0, propertyName.length - 2);
      }
      result.push(propertyName);
      for (let i = 0; i < arrayAccessCount; i++) {
        result.push(0);
      }
    }
    return result as PathArray;
  }
  return path;
}

function extractSingleValueOrUndefined(jsonValue: unknown, pathArray: PathArray): unknown {
  let result = jsonValue;
  for (const propertyName of pathArray) {
    if (!result || typeof result !== "object" || Array.isArray(result)) {
      return undefined;
    }
    assert(propertyName !== 0);
    result = (result as Record<string, unknown>)[propertyName];
  }
  return result;
}

function isJsonPrimitive(jsonValue: unknown): jsonValue is JsonPrimitive {
  const type = typeof jsonValue;
  return type === "number" || type === "string" || type === "boolean" || jsonValue === null;
}

function anyValueMatchesAtPath(
  jsonValue: unknown,
  pathArray: PathArray,
  predicate: (value: JsonPrimitive) => boolean,
  pathIndex = 0,
): boolean {
  if (pathIndex >= pathArray.length) {
    return isJsonPrimitive(jsonValue) && predicate(jsonValue);
  }

  const propertyNameOrZero = pathArray[pathIndex];
  if (propertyNameOrZero === 0) {
    if (!Array.isArray(jsonValue)) {
      return false;
    }
    return jsonValue.some((element) => anyValueMatchesAtPath(element, pathArray, predicate, pathIndex + 1));
  }

  if (!jsonValue || typeof jsonValue !== "object" || Array.isArray(jsonValue)) {
    return false;
  }
  return anyValueMatchesAtPath(
    (jsonValue as Record<string, unknown>)[propertyNameOrZero],
    pathArray,
    predicate,
    pathIndex + 1,
  );
}

function argumentsCompatible(a: unknown, b: unknown, onlyForRange: boolean): boolean {
  const aType = typeof a;
  const bType = typeof b;
  if (aType !== bType) {
    return false;
  }
  const stringOrNumber = aType === "string" || aType === "number";
  if (onlyForRange) {
    return stringOrNumber;
  }
  return stringOrNumber || aType === "boolean" || (a === null && b === null);
}

const OPERATOR_FUNCTIONS = new Map<Operator, (a: JsonPrimitive, b: unknown) => boolean>([
  ["=", (a, b) => a === b && argumentsCompatible(a, b, false)],
  ["<=", (a, b) => argumentsCompatible(a, b, true) && (a as any) <= (b as any)],
  [">=", (a, b) => argumentsCompatible(a, b, true) && (a as any) >= (b as any)],
  ["<", (a, b) => argumentsCompatible(a, b, true) && (a as any) < (b as any)],
  [">", (a, b) => argumentsCompatible(a, b, true) && (a as any) > (b as any)],
  ["in", (a, b) => Array.isArray(b) && b.includes(a)],
  ["is", (a, b) => typeof a === b],
  ["match", (a, b) => typeof b === "function" && isJsonPrimitive(a) && b(a)],
]);

function buildFilterPredicate(filterCondition: FilterCondition): (jsonValue: unknown) => boolean {
  const firstElement = filterCondition[0];
  if (Array.isArray(firstElement)) {
    // FilterCondition[]
    const predicates = filterCondition.map((condition) => {
      assert(Array.isArray(condition));
      return buildFilterPredicate(condition as FilterCondition);
    });
    return (jsonValue: unknown) => {
      for (const predicate of predicates) {
        if (!predicate(jsonValue)) {
          return false;
        }
      }
      return true;
    };
  } else if (firstElement === "or" && Array.isArray(filterCondition[1])) {
    const predicates = filterCondition.slice(1).map((condition) => {
      assert(Array.isArray(condition));
      return buildFilterPredicate(condition as FilterCondition);
    });
    return (jsonValue: unknown) => {
      for (const predicate of predicates) {
        if (predicate(jsonValue)) {
          return true;
        }
      }
      return false;
    };
  } else {
    let pathArray: PathArray;
    let operator: string;
    let argument: unknown;

    const length = filterCondition.length;
    if (length === 2) {
      const pathAndOperator = filterCondition[0];
      assert(typeof pathAndOperator === "string");
      const spaceIndex = pathAndOperator.lastIndexOf(" ");
      assert(spaceIndex >= 0);
      pathArray = pathToPathArray(pathAndOperator.substring(0, spaceIndex));
      operator = pathAndOperator.substring(spaceIndex + 1);
      argument = filterCondition[1];
    } else {
      assert(length >= 3);
      const pathParts = filterCondition.slice(0, -2);
      assert((pathParts as unknown[]).every((part) => typeof part === "string" || part === 0));
      pathArray = pathParts as PathArray;
      const tempOperator = filterCondition[length - 2];
      assert(typeof tempOperator === "string");
      operator = tempOperator;
      argument = filterCondition[length - 1];
    }

    const operatorFunction = OPERATOR_FUNCTIONS.get(operator as Operator);
    assert(operatorFunction, "unknown operator");
    const predicate = (value: JsonPrimitive) => operatorFunction(value, argument);
    return (jsonValue: unknown) => anyValueMatchesAtPath(jsonValue, pathArray, predicate);
  }
}

// T can just be specified, it is not validated...
export function queryJson<T extends object, P extends ProjectionType = undefined>(
  pageAccess: PageAccessNotUndefined,
  queryParameters: QueryParameters,
  projection?: P,
): QueryResult<P, T>;
export function queryJson<T extends object, P extends ProjectionType = undefined>(
  pageAccess: PageAccess,
  queryParameters: QueryParameters,
  projection?: P,
): QueryResult<P, T> | false;
export function queryJson<T extends object, P extends ProjectionType = undefined>(
  pageAccess: PageAccess,
  { table, filter, extraFilter, orderBy, limit, offset }: QueryParameters,
  projection?: P,
): QueryResult<P, T> | false {
  // TODO: this is a "naive" implementation, optimizations will be implemented later (e.g. using the index)
  let entries = readAllEntries<HasId>(pageAccess, table);
  if (!entries) {
    return false;
  }
  if (filter) {
    const filterPredicate = buildFilterPredicate(filter);
    entries = entries.filter((entry) => filterPredicate(entry));
  }
  if (extraFilter) {
    entries = entries.filter((entry) => extraFilter(entry));
  }

  if (orderBy && orderBy.length) {
    const orderByPathArrays = orderBy.map(pathToPathArray);
    for (const pathArray of orderByPathArrays) {
      if (pathArray.includes(0)) {
        throw new Error("orderBy does not allow arrays");
      }
    }

    entries.sort((a, b) => {
      let result = 0;
      for (const pathArray of orderByPathArrays) {
        const aValue = extractSingleValueOrUndefined(a, pathArray);
        const bValue = extractSingleValueOrUndefined(b, pathArray);
        result = compareJsonPrimitives(aValue, bValue);
        if (result !== null) {
          break;
        }
      }
      return result;
    });
  }

  assert(limit === undefined || limit >= 0);
  assert(offset === undefined || offset >= 0);
  if (limit !== undefined || offset !== undefined) {
    entries = entries.slice(offset ?? 0, limit !== undefined ? (offset ?? 0) + limit : undefined);
  }

  if (projection === "onlyId") {
    return entries.map((entry) => entry.id) as QueryResult<P, T>;
  } else if (Array.isArray(projection)) {
    throw "TODO";
  } else {
    return entries as QueryResult<P, T>;
  }
}

export function countJson(pageAccess: PageAccessNotUndefined, countParameters: CountParameters): number;
export function countJson(pageAccess: PageAccess, countParameters: CountParameters): number | false;
export function countJson(pageAccess: PageAccess, countParameters: CountParameters): number | false {
  // TODO: this can/should be optimized, but for now just counting the ids should be okay
  const ids = queryJson(pageAccess, countParameters, "onlyId");
  return ids ? ids.length : false;
}

function initializeIfNecessary(pageAccess: PageAccessDuringTransaction): void {
  if (!isInitialized(pageAccess.get)) {
    const zeroPage = uint8ArrayToDataView(pageAccess.getForUpdate(MAGIC_NUMBER_PAGE_NUMBER));
    zeroPage.setUint32(0, MAGIC_NUMBER_V1);

    let maxAllocated = MAX_ALLOCATED_PAGE_NUMBER;
    const initPageProvider: PageProviderForWrite = {
      getPage: (pageNumber: number) => pageAccess.get(pageNumber),
      getPageForUpdate: (pageNumber: number) => pageAccess.getForUpdate(pageNumber),
      allocateNewPage: () => ++maxAllocated,
      releasePage: (pageNumber: number) => {
        assert(false);
      },
    };

    const rootPageOne = allocateAndInitBtreeRootPage(initPageProvider);
    const rootPageTwo = allocateAndInitBtreeRootPage(initPageProvider);

    if (
      rootPageOne !== FREE_LIST_ROOT_PAGE_NUMBER ||
      rootPageTwo !== TABLES_ROOT_PAGE_NUMBER ||
      maxAllocated !== TABLES_ROOT_PAGE_NUMBER
    ) {
      throw new Error("init failed");
    }

    const maxAllocatedPage = uint8ArrayToDataView(pageAccess.getForUpdate(MAX_ALLOCATED_PAGE_NUMBER));
    maxAllocatedPage.setUint32(0, maxAllocated);
  }
}

export function deleteJson(pageAccess: PageAccessDuringTransaction, tableName: string, id: number): boolean {
  if (!isInitialized(pageAccess.get)) {
    // nothing to delete
    return false;
  }

  const pageProvider = toPageProviderForWrite(pageAccess);
  const tableInfo = getOrCreateTableInfo(pageProvider, tableName);

  return deleteJsonInternal(pageProvider, tableInfo, id);
}

function deleteJsonInternal(
  pageProvider: PageProviderForWrite,
  tableInfo: TableInfo,
  id: number,
  newIndexEntries?: Uint8ArraySet,
): boolean {
  try {
    const prefix = tupleToUint8Array(UINT32_TUPLE, [id]);
    const mainEntry = findFirstBtreeEntryWithPrefix(pageProvider.getPage, tableInfo.mainRoot, prefix);
    if (!mainEntry) {
      // does not exist
      // still handle newIndexEntries if given
      if (newIndexEntries) {
        updateIndexForJson(id, undefined, newIndexEntries, tableInfo.indexRoot, pageProvider);
      }
      return false;
    }

    // remove index entries or potentially update them to newIndexEntries (if given)
    const pathNumberToPath = createPathNumberToPath(tableInfo, pageProvider.getPage);
    const { events } = readEntryIdAndJsonEvents(mainEntry, tableInfo, pathNumberToPath, pageProvider.getPage);
    const indexEntries = buildIndexEntries(events);
    updateIndexForJson(id, indexEntries, newIndexEntries, tableInfo.indexRoot, pageProvider);

    // remove the entry
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
    const definitelyNewEntry = id === undefined;
    if (definitelyNewEntry) {
      const lastEntry = notFalse(findLastBtreeEntry(pageProvider.getPage, tableInfo.mainRoot));
      id = lastEntry ? readTuple(lastEntry, UINT32_TUPLE, 0).values[0] + 1 : 0;
    } else {
      if (!(typeof id === "number")) {
        throw new Error("id is not a number");
      }
    }
    if (id >>> 0 !== id) {
      throw new TypeError("id is not a uint32: " + id);
    }

    const jsonEvents: FullJsonEvent[] = [];
    const cache: JsonPathToNumberCache = new Map();
    let emptyObjectWithoutPathCount = 0;
    produceJsonEvents(
      json,
      (type, path, value) => {
        if (path === undefined && type === JSON_EMPTY_OBJECT) {
          // special case, it is an entirely empty object
          ++emptyObjectWithoutPathCount;
        } else {
          // json is an object so path can never be undefined
          assert(path !== undefined);
          const pathNumber = jsonPathToNumber(pageProvider, tableInfo.metaRoot, path, cache);
          jsonEvents.push({ path, pathNumber, type, value });
        }
      },
      filterId,
    );
    assert(emptyObjectWithoutPathCount === 0 || (emptyObjectWithoutPathCount === 1 && !jsonEvents.length));

    const newIndexEntries = buildIndexEntries(jsonEvents);
    if (definitelyNewEntry) {
      // just write the new index entries
      updateIndexForJson(id, undefined, newIndexEntries, tableInfo.indexRoot, pageProvider);
    } else {
      // combine the index update with the delete to only do necessary changes on the index entries
      // TODO: maybe optimize the whole update somehow by only doing necessary changes to the actual entry
      // the delete can return true or false, both are fine
      deleteJsonInternal(pageProvider, tableInfo, id, newIndexEntries);
    }

    const jsonEventsArray = serializeJsonEvents(jsonEvents, (event) => event.pathNumber);

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
          Math.min(offset + MAX_SINGLE_PAGE_JSON_EVENTS_ARRAY_LENGTH, zeroOrLength),
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

    if (definitelyNewEntry) {
      (json as HasId).id = id;
    }
  } catch (e) {
    assert(e !== MISSING_PAGE);
    throw e;
  }
}
