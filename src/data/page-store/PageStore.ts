import { shallowReadonly, shallowRef, ShallowRef } from "vue";
import { IndexPage } from "./internal/IndexPage";
import { PageGroupPage, pageNumberToPageGroupNumber, PAGES_PER_PAGE_GROUP } from "./internal/PageGroupPage";
import { Patch } from "./internal/Patch";
import { dataViewsEqual, readUint48FromDataView } from "./internal/util";
import { PageData } from "./PageData";
import { BackendPageAndVersion, BackendPageToStore, PageStoreBackend } from "./PageStoreBackend";

// require at least 4KB
const MIN_PAGE_SIZE = 1 << 12;
// max page size is 64KB to ensure that 16bit indexes are sufficient
const MAX_PAGE_SIZE = 1 << 16;
// page numbers are uint32 (we have to use Math.pow(), bit shift won't work)
const MAX_PAGE_NUMBER = Math.pow(2, 32) - 1;

// the backend page number of the index page
const INDEX_PAGE_NUMBER = -1;

// the size of the "header" (currently just the transaction id) of a page in its backend page
const PAGE_OVERHEAD = 6;

/**
 * Internal error type to signal that a retry is required.
 */
class RetryRequiredError extends Error {
  constructor(message?: string) {
    super(message);
  }
}

class BackendPage {
  private indexPage?: IndexPage;
  private pageGroupPage?: PageGroupPage;

  constructor(readonly page: BackendPageAndVersion | undefined, readonly pageNumber: number) {}

  getAsIndexPage(backendPageSize: number): IndexPage {
    let result = this.indexPage;
    if (!result) {
      if (this.pageNumber !== INDEX_PAGE_NUMBER) {
        throw new Error("not an index page number: " + this.pageNumber);
      }
      if (this.page) {
        result = new IndexPage(this.page.data);
        if (result.pageSize !== backendPageSize || result.pageSize !== this.page.data.byteLength) {
          throw new Error("pageSize does not match the actual page size");
        }
      } else {
        // built the initial index page
        result = new IndexPage(undefined);
        result.pageSize = backendPageSize;
      }
      this.indexPage = result;
    }
    return result;
  }

  getAsPageGroupPage(): PageGroupPage {
    let result = this.pageGroupPage;
    if (!result) {
      if (this.pageNumber > -2 || -this.pageNumber % 2 !== 0) {
        throw new Error("not a page group page number: " + this.pageNumber);
      }
      const pageGroupNumber = -this.pageNumber / 2 - 1;
      if (this.page) {
        result = new PageGroupPage(pageGroupNumber, this.page.data);
      } else {
        result = new PageGroupPage(pageGroupNumber, undefined);
      }
      this.pageGroupPage = result;
    }
    return result;
  }
}

function pageDataEqual(a: PageData, b: PageData): boolean {
  if (a.buffer === b.buffer) {
    return true;
  }
  if (a.buffer.byteLength !== b.buffer.byteLength) {
    return false;
  }
  return dataViewsEqual(a.dataView, b.dataView);
}

export type TransactionResult<T> =
  | {
      committed: false;
    }
  | {
      committed: true;
      resultValue: T;
    };

function assertValidPageNumber(pageNumber: number) {
  if (pageNumber < 0 || pageNumber > MAX_PAGE_NUMBER) {
    throw new Error("invalid pageNumber: " + pageNumber);
  }
}

function pageGroupNumberToBackendPageNumber(pageGroupNumber: number): number {
  return -2 - 2 * pageGroupNumber;
}

class PageEntry {
  readonly dataRef: ShallowRef<PageData | undefined>;
  readonly readonlyDataRef: Readonly<ShallowRef<PageData | undefined>>;

  dirty = false;

  constructor(readonly pageNumber: number) {
    assertValidPageNumber(pageNumber);
    this.dataRef = shallowRef();
    this.readonlyDataRef = shallowReadonly(this.dataRef);
  }

  setData(newPageData: PageData) {
    const oldPageData = this.dataRef.value;
    // re-set the dataRef if the data is different or if the page was marked as dirty
    if (oldPageData === undefined || this.dirty || !pageDataEqual(oldPageData, newPageData)) {
      this.dataRef.value = newPageData;
    }
    this.dirty = false;
  }
}

const RESOLVED_PROMISE = Promise.resolve();

export class PageStore {
  // TODO: use "MapWithWeakRefValues" to allow garbage collection...
  private readonly backendPages = new Map<number, BackendPage>();
  // TODO: use "MapWithWeakRefValues" to allow garbage collection...
  private readonly pageEntries = new Map<number, PageEntry>();

  /** Backend pages that will be loaded in the next microtask. */
  private readonly loadTriggeredPages = new Set<number>();
  /** All backend pages that are currently in "loading". */
  private readonly loadingPages = new Set<number>();
  /** The backend pages that were loaded. */
  private readonly loadedPages = new Set<number>();

  private loadingFinishedPromise?: Promise<void>;
  private loadingFinishedResolve?: () => void;

  private transactionActive = false;

  constructor(private readonly backend: PageStoreBackend) {
    const pageSize = backend.pageSize;
    if (pageSize < MIN_PAGE_SIZE) {
      throw new Error("backend pageSize is too small");
    }
    if (pageSize > MAX_PAGE_SIZE) {
      throw new Error("backend pageSize is too large");
    }
  }

  private get backendPageSize(): number {
    return this.backend.pageSize;
  }

  get pageSize(): number {
    return this.backendPageSize - PAGE_OVERHEAD;
  }

  get loading(): boolean {
    return !!this.loadingPages.size;
  }

  private getIndexPage(): IndexPage | undefined {
    return this.backendPages.get(INDEX_PAGE_NUMBER)?.getAsIndexPage(this.backendPageSize);
  }

  private getTransactionIdOfPageGroupPage(pageGroupNumber: number): number | undefined {
    const indexPage = this.getIndexPage();
    if (!indexPage) {
      return undefined;
    }
    const transactionId = indexPage.pageGroupNumberToTransactionId.get(pageGroupNumber) ?? 0;
    if (transactionId === 0 && indexPage.transactionIdsPageStoreTransactionId !== 0) {
      throw new Error("transactionIdsPageStoreTransactionId not yet implemented");
    }
    return transactionId;
  }

  private getPageGroupPage(pageGroupNumber: number): PageGroupPage | undefined {
    const expectedTransactionId = this.getTransactionIdOfPageGroupPage(pageGroupNumber);
    if (expectedTransactionId === undefined) {
      return undefined;
    }
    const backendPageNumber = pageGroupNumberToBackendPageNumber(pageGroupNumber);
    let backendPage = this.backendPages.get(backendPageNumber);
    if (expectedTransactionId === 0 && !backendPage) {
      // there is no persisted page group page, just set the backendPage to what it should be
      backendPage = new BackendPage(undefined, backendPageNumber);
      this.backendPages.set(backendPageNumber, backendPage);
    }
    const pageGroupPage = backendPage?.getAsPageGroupPage();
    if (pageGroupPage && expectedTransactionId !== pageGroupPage.transactionId) {
      return undefined;
    }
    return pageGroupPage;
  }

  private buildPageData(pageNumber: number): PageData | undefined {
    assertValidPageNumber(pageNumber);
    const indexPage = this.getIndexPage();
    if (!indexPage) {
      return undefined;
    }

    const pageGroupPage = this.getPageGroupPage(pageNumberToPageGroupNumber(pageNumber));
    if (!pageGroupPage) {
      return undefined;
    }

    const pageTransactionId = pageGroupPage.pageNumberToTransactionId.get(pageNumber);
    let pageData: PageData;
    if (pageTransactionId !== undefined) {
      const backendPageData = this.backendPages.get(pageNumber)?.page?.data;
      if (!backendPageData) {
        return undefined;
      }
      const transactionId = readUint48FromDataView(new DataView(backendPageData), 0);
      if (transactionId !== pageTransactionId) {
        return undefined;
      }
      pageData = new PageData(backendPageData.slice(PAGE_OVERHEAD));
    } else {
      pageData = new PageData(new ArrayBuffer(this.pageSize));
    }
    // apply page group page patches
    pageGroupPage.pageNumberToPatches.get(pageNumber)?.forEach((patch) => patch.applyTo(pageData.array));

    // apply index page patches
    indexPage.pageNumberToPatches.get(pageNumber)?.forEach((patch) => patch.applyTo(pageData.array));

    return pageData;
  }

  loadingFinished(): Promise<void> {
    if (!this.loading) {
      return RESOLVED_PROMISE;
    }
    let result = this.loadingFinishedPromise;
    if (!result) {
      result = this.loadingFinishedPromise = new Promise<void>((resolve) => {
        this.loadingFinishedResolve = resolve;
      });
    }
    return result;
  }

  private resetPageEntriesFromBackendPages(): void {
    if (this.loading) {
      throw new Error("loading");
    }

    // TODO: for now just reset all pages, but this should optimized to only reset pages that actually need it
    // first get all the data
    const entryToData = new Map<PageEntry, PageData>();
    this.pageEntries.forEach((entry, pageNumber) => {
      const pageData = this.buildPageData(pageNumber);
      if (pageData) {
        entryToData.set(entry, pageData);
      } else {
        // something is probably inconsistent, trigger a refresh for the page
        this.triggerLoad(pageNumber);
      }
    });

    // and then only update the pages if everything is available
    if (!this.loading) {
      entryToData.forEach((data, entry) => entry.setData(data));
    }
  }

  private async processLoad(backendPageNumbers: number[]): Promise<void> {
    // TODO: error handling?!
    const loadResults = await this.backend.loadPages(backendPageNumbers);
    backendPageNumbers.forEach((pageNumber, index) => {
      const loadResult = loadResults[index];
      this.backendPages.set(pageNumber, new BackendPage(loadResult, pageNumber));
      this.loadedPages.add(pageNumber);
    });

    if (this.loadingPages.size === this.loadedPages.size) {
      // all current loads are finished, update all page entries now
      if (this.loadTriggeredPages.size) {
        // should never happen
        throw new Error("loadTriggeredPages is not empty");
      }
      this.loadingPages.clear();
      this.loadedPages.clear();

      this.resetPageEntriesFromBackendPages();

      // we might be loading again here (resetPageEntriesFromBackendPages can trigger new loads)
      if (!this.loading) {
        this.loadingFinishedResolve?.();
        this.loadingFinishedResolve = undefined;
        this.loadingFinishedPromise = undefined;
      }
    }
  }

  private triggerLoad(backendPageNumber: number): void {
    if (this.loadingPages.has(backendPageNumber)) {
      // this page is already loading
      return;
    }
    const needNewTask = !this.loadTriggeredPages.size;
    this.loadTriggeredPages.add(backendPageNumber);
    this.loadingPages.add(backendPageNumber);

    if (needNewTask) {
      queueMicrotask(() => {
        const backendPageNumbers = [...this.loadTriggeredPages];
        this.loadTriggeredPages.clear();
        void this.processLoad(backendPageNumbers);
      });
    }

    // if it is a 0 or positive page, then also refresh the index page and page group page
    if (backendPageNumber >= 0) {
      this.triggerLoad(INDEX_PAGE_NUMBER);
      this.triggerLoad(pageGroupNumberToBackendPageNumber(pageNumberToPageGroupNumber(backendPageNumber)));
    }
  }

  getPage(pageNumber: number): Readonly<ShallowRef<PageData | undefined>> {
    let pageEntry = this.pageEntries.get(pageNumber);
    if (!pageEntry) {
      pageEntry = new PageEntry(pageNumber);
      this.pageEntries.set(pageNumber, pageEntry);
      if (!this.loading) {
        const pageData = this.buildPageData(pageNumber);
        if (pageData) {
          // if we are not loading and the page data is available (via patches etc.), then just set it
          pageEntry.setData(pageData);
        }
      }
      if (!pageEntry.dataRef.value) {
        this.triggerLoad(pageNumber);
      }
    }
    return pageEntry.readonlyDataRef;
  }

  /**
   * Triggers a refresh of all pages, to make sure that they are up to date with the pages in the backend.
   */
  refresh(): void {
    // trigger a load of the index page, if there are changes it will (recursively) trigger further loads
    this.triggerLoad(INDEX_PAGE_NUMBER);
  }

  /**
   * This method needs to be called before modifying a page during a transaction.
   *
   * Calling this method is only allowed during a transaction. If the page is not available (yet), then an internal
   * exception is thrown, that causes runTransaction() to retry the transaction after the page is loaded (unless the
   * specified retries are exhausted).
   */
  getPageDataForUpdate(pageNumber: number): PageData {
    if (!this.transactionActive) {
      throw new Error("no transaction active");
    }
    const result = this.getPage(pageNumber).value;
    if (!result) {
      throw new RetryRequiredError("page is not loaded");
    }

    this.pageEntries.get(pageNumber)!.dirty = true;
    return result;
  }

  private async commit(): Promise<boolean> {
    if (!this.transactionActive) {
      throw new Error("there is no transaction active");
    }

    const oldIndexPage = this.getIndexPage();
    if (!oldIndexPage) {
      throw new Error("index page not available");
    }
    const newIndexPage = new IndexPage(oldIndexPage);
    let changes = false;
    this.pageEntries.forEach((entry, pageNumber) => {
      const data = entry.dataRef.value;
      if (entry.dirty) {
        const oldPageData = this.buildPageData(pageNumber);
        if (!data || !oldPageData) {
          throw new Error("data or oldPageData not available");
        }
        const patches = Patch.createPatches(oldPageData.array, data.array, data.array.length);
        if (patches.length) {
          changes = true;
          newIndexPage.pageNumberToPatches.set(
            pageNumber,
            Patch.mergePatches([...(newIndexPage.pageNumberToPatches.get(pageNumber) ?? []), ...patches])
          );

          newIndexPage.maxPageNumber = Math.max(newIndexPage.maxPageNumber, pageNumber);
        }
      }
    });

    if (changes) {
      const transactionId = (newIndexPage.transactionId += 1);
      const pagesToStore: BackendPageToStore[] = [];

      // push changes down to the page group pages and individual pages as necessary
      while (newIndexPage.serializedLength > this.backendPageSize) {
        const largestPageGroupNumber = newIndexPage.determineLargestPageGroup();

        if (largestPageGroupNumber === undefined) {
          throw new Error("index page too large, but no largest page group");
        }
        const pageGroupPageBackendPageNumber = pageGroupNumberToBackendPageNumber(largestPageGroupNumber);
        const oldPageGroupPage = this.getPageGroupPage(largestPageGroupNumber);
        if (!oldPageGroupPage) {
          // this can actually happen, if none of the pages of that group were loaded...
          this.triggerLoad(pageGroupPageBackendPageNumber);
          return false;
        }

        const newPageGroupPage = new PageGroupPage(largestPageGroupNumber, oldPageGroupPage);
        newPageGroupPage.transactionId = transactionId;
        newIndexPage.pageGroupNumberToTransactionId.set(largestPageGroupNumber, transactionId);
        newIndexPage.movePageGroupDataToPageGroup(newPageGroupPage);
        newIndexPage;

        while (newPageGroupPage.serializedLength > this.backendPageSize) {
          // write data to pages that have the largest patches
          throw new Error("TODO...");
        }

        const newPageGroupPageBuffer = new ArrayBuffer(this.backendPageSize);
        newPageGroupPage.serialize(newPageGroupPageBuffer);
        pagesToStore.push({
          pageNumber: pageGroupPageBackendPageNumber,
          data: newPageGroupPageBuffer,
          previousVersion: this.backendPages.get(pageGroupPageBackendPageNumber)?.page?.version,
        });
      }

      const newIndexPageBuffer = new ArrayBuffer(this.backendPageSize);
      newIndexPage.serialize(newIndexPageBuffer);
      pagesToStore.push({
        pageNumber: INDEX_PAGE_NUMBER,
        data: newIndexPageBuffer,
        previousVersion: this.backendPages.get(INDEX_PAGE_NUMBER)?.page?.version,
      });

      if (pagesToStore.length) {
        const storeResult = await this.backend.storePages(pagesToStore);
        if (!storeResult) {
          return false;
        }

        // update backendPages
        storeResult.forEach((newVersion, index) => {
          const pageToStore = pagesToStore[index];
          this.backendPages.set(
            pageToStore.pageNumber,
            new BackendPage({ data: pageToStore.data, version: newVersion }, pageToStore.pageNumber)
          );
        });
      }
    }

    // the transaction is completed
    this.transactionActive = false;
    this.resetPageEntriesFromBackendPages();

    return true;
  }

  async runTransaction<T>(transactionFn: () => T, retries?: number): Promise<TransactionResult<T>> {
    if (this.loading) {
      await this.loadingFinished();
    }

    if (this.transactionActive) {
      throw new Error("there is already an active transaction");
    }
    this.transactionActive = true;

    let result: TransactionResult<T> = {
      committed: false,
    };

    try {
      // TODO: make sure there is some kind of progress in each iteration, abort otherwise
      for (let retry = 0; retries === undefined || retry <= retries; ++retry) {
        if (retry > 0) {
          // we are in a retry, so refresh first
          this.refresh();
          await this.loadingFinished();
        }

        let resultValue: T;
        try {
          resultValue = transactionFn();
        } catch (e) {
          if (e instanceof RetryRequiredError) {
            // just retry
            continue;
          } else {
            // rethrow
            throw e;
          }
        }

        if (await this.commit()) {
          result = {
            committed: true,
            resultValue,
          };
          break;
        }
      }
    } finally {
      this.transactionActive = false;

      if (!result.committed) {
        // refresh and wait until loading is finished
        this.refresh();
        await this.loadingFinished();
      }
    }

    return result;
  }
}
