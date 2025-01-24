import { type MaybeRefOrGetter, onScopeDispose, reactive, type Ref, ref, toRaw, toValue, watch } from "vue";
import { queryJson, type QueryParameters } from "json-store";
import { PageStore, type PageReadsRecorder } from "page-store";

function extractId(value: unknown): number | undefined {
  if (typeof value === "object" && value !== null) {
    const id = (value as Record<string, unknown>).id;
    if (typeof id === "number") {
      return id;
    }
  }
  return undefined;
}

function buildIdMap(array: unknown[]): Map<number, unknown> | undefined {
  let result: Map<number, unknown> | undefined = undefined;
  for (const entry of array) {
    const id = extractId(entry);
    if (id === undefined) {
      // not all entries have an id
      return undefined;
    }
    if (!result) {
      // lazy init
      result = new Map();
    }
    if (result.has(id)) {
      // not all ids are distinct
      return undefined;
    }
    result.set(id, entry);
  }
  return result;
}

/**
 * existingValue is expected to be the raw value (not proxied), if any modifications are made then the proxy is
 * obtained (via reactive()) and used to do the modification. Doing it this way avoids the (significant) proxy overhead
 * for all reads, but modifications can still be observed reactively.
 */
function mergeInto(existingValue: unknown, newValue: unknown): unknown {
  if (existingValue === newValue) {
    return existingValue;
  }
  if (typeof existingValue === "object" && typeof newValue === "object") {
    if (Array.isArray(newValue)) {
      if (Array.isArray(existingValue)) {
        const length = newValue.length;
        let existingValueReactive: any[] | undefined = undefined;
        // merge array values
        if (length) {
          const idMap = buildIdMap(existingValue);
          if (idMap) {
            // reuse existing values by id
            for (let i = 0; i < length; i++) {
              const newEntry = newValue[i];
              const id = extractId(newEntry);
              const existingEntry = id !== undefined ? idMap.get(id) : undefined;
              const mergedEntry = mergeInto(existingEntry, newEntry);
              if (existingValue[i] !== mergedEntry) {
                existingValueReactive ??= reactive(existingValue);
                existingValueReactive[i] = mergedEntry;
              }
            }
          } else {
            // reuse existing values by position
            for (let i = 0; i < length; i++) {
              const existingEntry = existingValue[i];
              const mergedEntry = mergeInto(existingEntry, newValue[i]);
              if (existingEntry !== mergedEntry) {
                existingValueReactive ??= reactive(existingValue);
                existingValueReactive[i] = mergedEntry;
              }
            }
          }
        }
        if (existingValue.length !== length) {
          existingValueReactive ??= reactive(existingValue);
          existingValueReactive.length = length;
        }
        return existingValue;
      }
    } else if (existingValue && newValue && !Array.isArray(existingValue)) {
      // both existingValue and newValue are objects
      const existingRecord = existingValue as Record<string, unknown>;
      let existingRecordReactive: Record<string, unknown> | undefined = undefined;
      const newRecord = newValue as Record<string, unknown>;
      const unhandledKeys = new Set(Object.keys(existingRecord));
      for (const [key, newEntryValue] of Object.entries(newRecord)) {
        if (newEntryValue !== undefined) {
          const existingEntryValue = existingRecord[key];
          const mergedEntryValue = mergeInto(existingEntryValue, newEntryValue);
          if (existingEntryValue !== mergedEntryValue) {
            existingRecordReactive ??= reactive(existingRecord);
            existingRecordReactive[key] = mergedEntryValue;
          }
          unhandledKeys.delete(key);
        }
      }
      if (unhandledKeys.size) {
        existingRecordReactive ??= reactive(existingRecord);
        for (const key of unhandledKeys) {
          delete existingRecordReactive[key];
        }
      }
      return existingRecord;
    }
  }

  // the two values cannot be merged, just return newValue
  return newValue;
}

export function useJsonQuery<T extends object>(
  pageStore: MaybeRefOrGetter<PageStore | undefined>,
  queryParameters: MaybeRefOrGetter<QueryParameters>,
): Ref<T[] | false> {
  // this ref is used to trigger a recompute
  const invalidateToggle = ref(false);
  let lastInvalidateToggleValue = false;
  const invalidateCallback = () => {
    invalidateToggle.value = !lastInvalidateToggleValue;
  };
  let lastPageStore: PageStore | undefined = undefined;
  let pageReadsRecorder: PageReadsRecorder | undefined = undefined;
  const cleanup = () => {
    // cleanup by recording no page reads
    pageReadsRecorder?.(() => {});
    pageReadsRecorder = undefined;
  };
  onScopeDispose(cleanup);

  toValue;

  const result: Ref<T[] | false> = ref(false);

  watch(
    [invalidateToggle, () => toValue(pageStore), () => toValue(queryParameters)],
    ([invalidateToggleValue, currentPageStore, currentParameters]) => {
      lastInvalidateToggleValue = invalidateToggleValue;

      if (!currentPageStore) {
        cleanup();
        result.value = false;
        lastPageStore = undefined;
      } else {
        if (lastPageStore !== currentPageStore || !pageReadsRecorder) {
          cleanup();
          pageReadsRecorder = currentPageStore.getPageReadsRecorder(invalidateCallback);
        }
        lastPageStore = currentPageStore;

        const label = JSON.stringify(currentParameters);
        console.time("recompute " + label);
        const queryResult = pageReadsRecorder((pageAccess) => queryJson<T>(pageAccess, currentParameters));
        console.timeEnd("recompute " + label);
        if (!result.value) {
          result.value = queryResult;
        } else {
          if (queryResult) {
            console.time("merge " + label);
            mergeInto(toRaw(result.value), queryResult);
            console.timeEnd("merge " + label);
          } else {
            // just keep the existing result if the query goes back to false
          }
        }
      }
    },
    { immediate: true },
  );

  return result;
}
