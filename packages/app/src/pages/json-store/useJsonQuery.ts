import { onScopeDispose, type Ref, ref, watch } from "vue";
import { queryJson } from "../../data/json/jsonStore";
import { PageStore } from "../../data/page-store/PageStore";
import { type QueryParameters } from "../../data/json/queryTypes";

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

function mergeInto(existingValue: unknown, newValue: unknown): unknown {
  if (existingValue === newValue) {
    return existingValue;
  }
  if (typeof existingValue === "object" && typeof newValue === "object") {
    if (Array.isArray(newValue)) {
      if (Array.isArray(existingValue)) {
        const length = newValue.length;
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
                existingValue[i] = mergedEntry;
              }
            }
          } else {
            // reuse existing values by position
            for (let i = 0; i < length; i++) {
              const existingEntry = existingValue[i];
              const mergedEntry = mergeInto(existingEntry, newValue[i]);
              if (existingEntry !== mergedEntry) {
                existingValue[i] = mergedEntry;
              }
            }
          }
        }
        existingValue.length = length;
        return existingValue;
      }
    } else if (existingValue && newValue && !Array.isArray(existingValue)) {
      // both existingValue and newValue are objects
      const existingRecord = existingValue as Record<string, unknown>;
      const newRecord = newValue as Record<string, unknown>;
      const unhandledKeys = new Set(Object.keys(existingRecord));
      for (const [key, newEntryValue] of Object.entries(newRecord)) {
        if (newEntryValue !== undefined) {
          const existingEntryValue = existingRecord[key];
          const mergedEntryValue = mergeInto(existingEntryValue, newEntryValue);
          if (existingEntryValue !== mergedEntryValue) {
            existingRecord[key] = mergedEntryValue;
          }
          unhandledKeys.delete(key);
        }
      }
      unhandledKeys.forEach((key) => delete existingRecord[key]);
      return existingRecord;
    }
  }

  // the two values cannot be merged, just return newValue
  return newValue;
}

export function useJsonQuery<T extends object>(
  pageStore: PageStore,
  queryParameters: () => QueryParameters
): Ref<T[] | false> {
  // this ref is used to trigger a recompute
  const invalidateToggle = ref(false);
  let lastInvalidateToggleValue = false;
  const invalidateCallback = () => {
    invalidateToggle.value = !lastInvalidateToggleValue;
  };
  const pageReadsRecorder = pageStore.getPageReadsRecorder(invalidateCallback);
  onScopeDispose(() => {
    // cleanup by recording no page reads
    pageReadsRecorder(() => {});
  });

  const result: Ref<T[] | false> = ref(false);

  watch(
    [invalidateToggle, queryParameters],
    ([invalidateToggleValue, currentParameters]) => {
      lastInvalidateToggleValue = invalidateToggleValue;
      const label = JSON.stringify(currentParameters);
      console.time("recompute " + label);
      const queryResult = pageReadsRecorder((pageAccess) => queryJson<T>(pageAccess, currentParameters));
      console.timeEnd("recompute " + label);
      if (!result.value) {
        result.value = queryResult;
      } else {
        if (queryResult) {
          console.time("merge " + label);
          mergeInto(result.value, queryResult);
          console.timeEnd("merge " + label);
        } else {
          // just keep the existing result if the query goes back to false
        }
      }
    },
    { immediate: true }
  );

  return result;
}
