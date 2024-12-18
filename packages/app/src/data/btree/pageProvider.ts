/**
 * @returns the page if it is loaded/available, otherwise undefined is returned.
 */
export type PageProvider = (pageNumber: number) => Uint8Array | undefined;

/**
 * All methods of this interface are expected to always return the value/execute the operation. If that is not possible
 * for some reason, then they should throw an Error.
 */
export interface PageProviderForWrite {
  /**
   * Multiple calls with the same pageNumber generally return the same Uint8Array, but after a call to
   * getPageForUpdate() for the page, this method will also return the Uint8Array returned by getPageForUpdate() (which
   * might be different from the Uint8Array returned before), the previously returned Uint8Array will then not reflect
   * the changes that were made to the Uint8Array returned by getPageForUpdate().
   *
   * @returns the page
   */
  getPage(pageNumber: number): Uint8Array;

  /**
   * This is like getPage(), but this method is used, when the page will be updated and not just read. This method
   * might return a different Uint8Array from the one that was returned by getPage() (but the content will initially be
   * equal).
   *
   * @returns the page
   */
  getPageForUpdate(pageNumber: number): Uint8Array;

  /**
   * Allocates/reserves a new page to be used by the B-tree and returns its pageNumber.
   */
  allocateNewPage(): number;

  /**
   * Releases a page that was previously allocated via allocateNewPage() back into the "pool".
   */
  releasePage(pageNumber: number): void;
}
