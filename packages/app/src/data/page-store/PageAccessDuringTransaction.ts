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
   * getForUpdate() needs to be used.
   *
   * Multiple calls with the same pageNumber generally return the same Uint8Array, but after a call to getForUpdate()
   * for the page, this method will also return the Uint8Array returned by getForUpdate() (which might be different
   * from the Uint8Array returned before), the previously returned Uint8Array will then not reflect
   * the changes that were made to the Uint8Array returned by getForUpdate().
   */
  get(pageNumber: number): Uint8Array;

  /**
   * Like get(), but also marks the page as "dirty" and the caller is allowed to modify/update the returned Uint8Array.
   *
   * This method might return a different Uint8Array from the one that was returned by get() (but the content will
   * initially be equal).
   */
  getForUpdate(pageNumber: number): Uint8Array;
}
