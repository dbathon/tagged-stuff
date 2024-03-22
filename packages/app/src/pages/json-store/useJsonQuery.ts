import { computed, type ComputedRef, onScopeDispose, ref } from "vue";
import { queryJson } from "../../data/json/jsonStore";
import { PageStore } from "../../data/page-store/PageStore";
import { type QueryParameters } from "../../data/json/queryTypes";

export function useJsonQuery<T extends object>(
  pageStore: PageStore,
  queryParameters: () => QueryParameters
): ComputedRef<T[] | false> {
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

  return computed(() => {
    // read invalidateToggle here to recompute if it changes
    lastInvalidateToggleValue = invalidateToggle.value;
    const currentParameters = queryParameters();
    const label = "recompute " + JSON.stringify(currentParameters);
    console.time(label);
    const result = pageReadsRecorder((pageAccess) => queryJson<T>(pageAccess, currentParameters));
    console.timeEnd(label);
    return result;
  });
}
