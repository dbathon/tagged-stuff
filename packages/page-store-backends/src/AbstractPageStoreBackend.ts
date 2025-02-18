import type {
  BackendIndexPage,
  BackendPage,
  BackendPageIdentifier,
  BackendReadResult,
  PageStoreBackend,
} from "page-store";

export abstract class AbstractPageStoreBackend implements PageStoreBackend {
  // Support up to 64KB, this limit is arbitrary, it could be increased...
  readonly maxPageSize = 1 << 16;

  protected abstract fetchBackendIndexPage(): Promise<BackendIndexPage | undefined>;

  protected abstract fetchBackendPage(identifier: BackendPageIdentifier): Promise<BackendPage | undefined>;

  protected async fetchBackendIndexPageOrInitial(): Promise<BackendIndexPage> {
    const result = await this.fetchBackendIndexPage();
    if (result) {
      return result;
    } else {
      // return initial index page
      return {
        transactionId: 0,
        data: new Uint8Array(0),
      };
    }
  }

  async readPages(includeIndexPage: boolean, pageIdentifiers: BackendPageIdentifier[]): Promise<BackendReadResult> {
    // fetch everything in parallel
    const indexPagePromise: Promise<BackendIndexPage | undefined> = includeIndexPage
      ? this.fetchBackendIndexPageOrInitial()
      : Promise.resolve(undefined);
    const pagesPromise: Promise<(BackendPage | undefined)[]> = Promise.all(
      pageIdentifiers.map((identifier) => this.fetchBackendPage(identifier)),
    );

    const [indexPage, pages] = await Promise.all([indexPagePromise, pagesPromise]);

    return {
      indexPage,
      pages: pages.filter((page) => !!page),
    };
  }

  /**
   * @returns true on success, false for conflicts (page already exists) and throws in other cases
   */
  protected abstract writeBackendPage(page: BackendPage): Promise<boolean>;

  protected abstract writeIndexPage(indexPage: BackendIndexPage, previousTransactionId: number): Promise<boolean>;

  protected abstract cleanupObsoleteBackendPages(writtenPages: BackendPage[]): Promise<void>;

  async writePages(indexPage: BackendIndexPage, previousTransactionId: number, pages: BackendPage[]): Promise<boolean> {
    // TODO: batching etc.
    for (const page of pages) {
      const success = await this.writeBackendPage(page);
      if (!success) {
        // TODO: maybe cleanup already written pages?
        return false;
      }
    }

    // all backend pages were inserted successfully, so now try the index page update
    const indexUpdateSuccess = await this.writeIndexPage(indexPage, previousTransactionId);
    if (!indexUpdateSuccess) {
      return false;
    }

    // the "commit" was successful, fire deletes for all potentially obsolete pages
    // we don't wait for the requests, it is not a big problem, if they fail or are not completed
    void this.cleanupObsoleteBackendPages(pages);

    return true;
  }
}
