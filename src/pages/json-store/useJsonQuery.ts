import { computed, ComputedRef } from "vue";
import { PageAccess, queryJson, QueryParameters } from "../../data/json/jsonStore";
import { PageStore } from "../../data/page-store/PageStore";

export function useJsonQuery<T>(
  pageStore: PageStore,
  tableName: string,
  queryParameters: QueryParameters = {}
): ComputedRef<T[] | false> {
  const pageAccess: PageAccess = (pageNumber) => pageStore.getPage(pageNumber).value;
  return computed(() => {
    console.log("recompute", tableName);
    return queryJson<T>(pageAccess, tableName, queryParameters);
  });
}
