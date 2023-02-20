import { PageData } from "./PageData";

/**
 * Provides access to pages during a transaction.
 */
export interface PageAccessDuringTransaction {
  /**
   * Always returns the PageData. If the page is not available (yet), then an internal exception is thrown, that causes
   * runTransaction() to retry the transaction after the page is loaded (unless the specified retries are exhausted).
   *
   * The caller is not allowed to modify the returned PageData, if the PageData needs to be updated, then
   * getForUpdate() should be used.
   */
  get(pageNumber: number): PageData;

  /**
   * Like get(), but also marks the page as "dirty" and the caller is allowed to modify/update the PageData.
   */
  getForUpdate(pageNumber: number): PageData;
}
