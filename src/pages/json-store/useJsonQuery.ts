import { computed, ComputedRef } from "vue";
import { PageAccess, queryJson } from "../../data/json/jsonStore";
import { PageStore } from "../../data/page-store/PageStore";
import { QueryParameters } from "../../data/json/queryTypes";

export function useJsonQuery<T extends object>(
  pageStore: PageStore,
  queryParameters: () => QueryParameters
): ComputedRef<T[] | false> {
  const pageAccess: PageAccess = (pageNumber) => pageStore.getPage(pageNumber).value;
  return computed(() => {
    const currentParameters = queryParameters();
    const label = "recompute " + JSON.stringify(currentParameters);
    console.time(label);
    const result = queryJson<T>(pageAccess, currentParameters);
    console.timeEnd(label);
    return result;
  });
}
