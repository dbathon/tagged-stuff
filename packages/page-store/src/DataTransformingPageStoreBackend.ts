import type {
  BackendIndexPage,
  BackendPage,
  BackendPageIdentifier,
  BackendReadResult,
  PageStoreBackend,
} from "./PageStoreBackend";

export abstract class DataTransformingPageStoreBackend implements PageStoreBackend {
  constructor(protected readonly underlyingBackend: PageStoreBackend) {}

  protected abstract readonly maxPageSizeOverhead: number;

  get maxPageSize(): number {
    return this.underlyingBackend.maxPageSize - this.maxPageSizeOverhead;
  }

  /** Used by writePages(). */
  protected abstract transform(data: Uint8Array): Promise<Uint8Array>;

  /** Used by readPages(). */
  protected abstract reverseTransform(transformedData: Uint8Array): Promise<Uint8Array>;

  async readPages(includeIndexPage: boolean, pageIdentifiers: BackendPageIdentifier[]): Promise<BackendReadResult> {
    const underlyingResponse = await this.underlyingBackend.readPages(includeIndexPage, pageIdentifiers);
    let indexPage: BackendIndexPage | undefined = undefined;
    const underlyingIndexPage = underlyingResponse.indexPage;
    if (underlyingIndexPage) {
      indexPage = {
        transactionId: underlyingIndexPage.transactionId,
        // empty data is a special case for uninitialized store, just pass it through as is
        data:
          underlyingIndexPage.transactionId === 0 && underlyingIndexPage.data.length === 0
            ? underlyingIndexPage.data
            : await this.reverseTransform(underlyingIndexPage.data),
      };
    }

    const pages = await Promise.all(
      underlyingResponse.pages.map(async (page): Promise<BackendPage> => {
        return {
          identifier: page.identifier,
          data: await this.reverseTransform(page.data),
        };
      }),
    );

    return { indexPage, pages };
  }

  async writePages(indexPage: BackendIndexPage, previousTransactionId: number, pages: BackendPage[]): Promise<boolean> {
    const transformedIndexPage: BackendIndexPage = {
      transactionId: indexPage.transactionId,
      data: await this.transform(indexPage.data),
    };

    const transformedPages = await Promise.all(
      pages.map(async (page): Promise<BackendPage> => {
        return {
          identifier: page.identifier,
          data: await this.transform(page.data),
        };
      }),
    );

    return this.underlyingBackend.writePages(transformedIndexPage, previousTransactionId, transformedPages);
  }
}
