import { BackendPageAndVersion, BackendPageToStore, PageStoreBackend } from "./PageStoreBackend";

/**
 * A simple in memory implementation of PageStoreBackend for testing.
 */
export class InMemoryPageStoreBackend implements PageStoreBackend {
  private readonly pages = new Map<number, BackendPageAndVersion>();

  constructor(readonly pageSize: number) {}

  async loadPages(pageIndexes: number[]): Promise<(BackendPageAndVersion | undefined)[]> {
    return pageIndexes.map((index) => {
      const pageAndVersion = this.pages.get(index);
      if (!pageAndVersion) {
        return undefined;
      } else {
        // return a copy of the internal data
        return {
          data: pageAndVersion.data.slice(0),
          version: pageAndVersion.version,
        };
      }
    });
  }

  async storePages(pages: BackendPageToStore[]): Promise<number[] | undefined> {
    // defer all the updates to the end
    const updates: (() => void)[] = [];
    const result: number[] = [];

    const seenIndexes = new Set<number>();
    for (const { pageIndex, data, previousVersion } of pages) {
      if (data.byteLength !== this.pageSize) {
        throw new Error("invalid byteLength: " + data.byteLength);
      }
      if (seenIndexes.has(pageIndex)) {
        throw new Error("duplicate pageIndex: " + pageIndex);
      }
      seenIndexes.add(pageIndex);
      const existingPage = this.pages.get(pageIndex);
      if (!existingPage) {
        if (previousVersion !== undefined) {
          return undefined;
        }
        updates.push(() => this.pages.set(pageIndex, { data: data.slice(0), version: 0 }));
        result.push(0);
      } else {
        if (previousVersion !== existingPage.version) {
          return undefined;
        }
        const newVersion = existingPage.version + 1;
        updates.push(() => {
          existingPage.data = data.slice(0);
          existingPage.version = newVersion;
        });
        result.push(newVersion);
      }
    }

    for (const update of updates) {
      update();
    }
    return result;
  }
}
