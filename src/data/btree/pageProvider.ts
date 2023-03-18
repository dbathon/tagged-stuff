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
   * @returns the page
   */
  getPage(pageNumber: number): Uint8Array;

  /**
   * This is like getPage(), but this method is used, when the page will be updated and not just read.
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
