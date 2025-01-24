import {
  type BackendIndexPage,
  type BackendPage,
  type BackendPageIdentifier,
  type BackendReadResult,
  type PageStoreBackend,
} from "./PageStoreBackend";

/**
 * A simple in memory implementation of PageStoreBackend for testing.
 */
export class InMemoryPageStoreBackend implements PageStoreBackend {
  // Support up to 128KB, this limit is arbitrary, it could be increased...
  readonly maxPageSize = 1 << 17;

  indexPage: BackendIndexPage = {
    transactionId: 0,
    data: new Uint8Array(0),
  };

  readonly pages = new Map<number, BackendPage>();

  constructor() {}

  async readPages(includeIndexPage: boolean, pageIdentifiers: BackendPageIdentifier[]): Promise<BackendReadResult> {
    const pages: BackendPage[] = [];
    for (const pageIdentifier of pageIdentifiers) {
      const page = this.pages.get(pageIdentifier.pageNumber);
      if (page && page.identifier.transactionId === pageIdentifier.transactionId) {
        pages.push({
          identifier: { ...page.identifier },
          data: page.data.slice(0),
        });
      }
    }

    return {
      indexPage: includeIndexPage
        ? {
            transactionId: this.indexPage.transactionId,
            data: this.indexPage.data.slice(0),
          }
        : undefined,
      pages,
    };
  }

  async writePages(indexPage: BackendIndexPage, previousTransactionId: number, pages: BackendPage[]): Promise<boolean> {
    // first do all the conflict checks
    if (previousTransactionId !== this.indexPage.transactionId) {
      return false;
    }
    for (const page of pages) {
      const existingPage = this.pages.get(page.identifier.pageNumber);
      if (existingPage && existingPage.identifier.transactionId === page.identifier.transactionId) {
        return false;
      }
    }

    // do some other validations
    if (indexPage.transactionId <= previousTransactionId) {
      throw new Error("invalid index page transactionId");
    }
    // maybe more?!

    for (const page of pages) {
      this.pages.set(page.identifier.pageNumber, {
        identifier: {
          pageNumber: page.identifier.pageNumber,
          transactionId: page.identifier.transactionId,
        },
        data: page.data.slice(0),
      });
    }
    this.indexPage = {
      transactionId: indexPage.transactionId,
      data: indexPage.data.slice(0),
    };

    return true;
  }
}
