export interface BackendPageIdentifier {
  readonly pageNumber: number;
  readonly transactionId: number;
}

export interface BackendPage {
  readonly identifier: BackendPageIdentifier;
  readonly data: Uint8Array;
}

export interface BackendIndexPage {
  readonly transactionId: number;
  readonly data: Uint8Array;
}

export interface BackendReadResult {
  readonly indexPage?: BackendIndexPage;
  readonly pages: BackendPage[];
}

/**
 * A page store backend stores pages of bytes, all pages can have different sizes, but there is a maximum size a
 * backend supports.
 *
 * The pageNumber of each backend page is an integer (greater than or equal to 0, uint32). There is one special page:
 * the index page, which does not have a pageNumber. Normal pages are stored with a "transaction id" (uint48) and when
 * pages are read the expected transaction id is also requested. The backend must only return the requested page if the
 * transaction id matches. The index page also has a transaction id, it is basically the transaction id of the whole
 * store backend.
 *
 * An "uninitialized" store backend always starts with transaction id 0, an empty index page (length 0) and no other
 * pages.
 *
 * Writes/updates always change the index page and optionally one or more normal pages. The backend needs to ensure
 * that concurrent writes don't conflict with each other: basically all page updates need to happen atomically, either
 * all pages are updated (including the index page) or none.
 *
 * The backend is only expected to keep the latest version of each page that was successfully written. Earlier versions
 * of pages can be discarded. The transaction id is always increasing, later writes never user smaller transaction ids.
 *
 * Page store backends can also be layered/wrapped: there could for example be an actual backend that handles the
 * storage and another one that wraps the first one and implements encryption or compression on top of it.
 */
export interface PageStoreBackend {
  /**
   * The maximum size (in bytes) of pages that can be stored by this backend.
   */
  readonly maxPageSize: number;

  /**
   * If includeIndexPage is true, then indexPage will be set in the result. The result might not include all pages
   * specified in pageIdentifiers (and they might be in a different order): if a page does not exist with the requested
   * transaction id, then it is just omitted from the result.
   */
  readPages(includeIndexPage: boolean, pageIdentifiers: BackendPageIdentifier[]): Promise<BackendReadResult>;

  /**
   * Returns true if the write was successful or false if the write could not be performed due to a conflict and should
   * be retried (after reading the latest index page and other needed pages). If any other error happens, then an
   * exception is thrown.
   *
   * @param indexPage the new index page with the new transactionId
   * @param previousTransactionId
   *   the previous transaction id, the write will only succeed if no newer transaction has been written in the mean
   *   time
   * @param pages
   *   the backend pages that need to be written for the transaction, generally the transactionId of the pages should
   *   match the transactionId of indexPage, the write will fail if there is already a version of a page with the given
   *   transactionId
   */
  writePages(indexPage: BackendIndexPage, previousTransactionId: number, pages: BackendPage[]): Promise<boolean>;
}
