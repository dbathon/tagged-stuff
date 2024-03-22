/**
 * Provides access to pages during a transaction.
 */
export interface PageAccessDuringTransaction {
  /**
   * Always returns the Uint8Array. If the page is not available (yet), then an internal exception is thrown, that
   * causes runTransaction() to retry the transaction after the page is loaded (unless the specified retries are
   * exhausted).
   *
   * The caller is not allowed to modify the returned Uint8Array, if the Uint8Array needs to be updated, then
   * getForUpdate() should be used.
   */
  get(pageNumber: number): Uint8Array;

  /**
   * Like get(), but also marks the page as "dirty" and the caller is allowed to modify/update the Uint8Array.
   */
  getForUpdate(pageNumber: number): Uint8Array;
}
