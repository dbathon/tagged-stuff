export interface BackendPageAndVersion {
  /**
   * This Uint8Array is considered to be constant, the PageStore will directly use this array so it should never change
   * after it is returned from loadPages().
   */
  data: Uint8Array;
  version: number;
}

export interface BackendPageToStore {
  pageNumber: number;
  data: Uint8Array;
  /** If it is a new page, then this needs to be set to undefined. */
  previousVersion: number | undefined;
}

/**
 * A page store backend stores pages of bytes, all pages have the same size.
 *
 * The pageNumber of each page is an integer (can also be negative or zero).
 *
 * The backend is also responsible for implementing "optimistic locking" using versions and atomic updates of multiple
 * pages at once.
 *
 * Page store backends can also be layered/wrapped: there could for example be an actual backend that handles the
 * storage and another one that wraps the first one and implements encryption and maybe compression on top of it.
 */
export interface PageStoreBackend {
  /**
   * The size (in bytes) of pages stored by this backend. This number needs to be constant and all pages must have the
   * same size.
   */
  readonly pageSize: number;

  loadPages(pageNumbers: number[]): Promise<(BackendPageAndVersion | undefined)[]>;

  /**
   * Returns the new versions of the stored pages. If undefined is returned, then there was an "optimistic lock
   * exception", i.e. the previousVersion of a page did not match.
   */
  storePages(pages: BackendPageToStore[]): Promise<number[] | undefined>;
}
