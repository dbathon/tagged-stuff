import { shallowReadonly, shallowRef, ShallowRef, triggerRef } from "vue";
import { IndexPage } from "./internal/IndexPage";
import { PageGroupPage, pageNumberToPageGroupNumber } from "./internal/PageGroupPage";
import { Patch } from "./internal/Patch";
import { readUint48FromDataView, writeUint48toDataView } from "./internal/util";
import { PageAccessDuringTransaction } from "./PageAccessDuringTransaction";
import { BackendPageAndVersion, BackendPageToStore, PageStoreBackend } from "./PageStoreBackend";
import { uint8ArrayToDataView } from "../uint8-array/uint8ArrayToDataView";
import { assert } from "../misc/assert";

// require at least 4KB
const MIN_PAGE_SIZE = 1 << 12;
// max page size is 64KB to ensure that 16bit indexes are sufficient
const MAX_PAGE_SIZE = 1 << 16;
// page numbers are uint32
const MAX_PAGE_NUMBER = -1 >>> 0;

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

/**
 * Contains information that is necessary to check whether the page data might have changed. If two PageEntryKeys are
 * equal for a page, then the page cannot have changed, if they are not, then it might have changed.
 */
class PageEntryKey {
  constructor(
    readonly indexPageTransactionId: number,
    readonly pageGroupTransactionId: number,
    readonly indexPagePatches: Patch[] | undefined
  ) {}

  equals(other: PageEntryKey): boolean {
    return (
      this.indexPageTransactionId === other.indexPageTransactionId &&
      this.pageGroupTransactionId === other.pageGroupTransactionId &&
      Patch.patchesEqual(this.indexPagePatches, other.indexPagePatches)
    );
  }
}

class PageEntry {
  readonly array: Uint8Array;
  readonly dataRef: ShallowRef<Uint8Array | undefined>;
  readonly readonlyDataRef: Readonly<ShallowRef<Uint8Array | undefined>>;
  /** Optional, if it is set, then it can be used to avoid rebuilding the data on refresh. */
  pageEntryKey?: PageEntryKey;

  constructor(readonly pageNumber: number, pageSize: number) {
    assertValidPageNumber(pageNumber);
    this.array = new Uint8Array(pageSize);
    this.dataRef = shallowRef();
    this.readonlyDataRef = shallowReadonly(this.dataRef);
  }

  arrayUpdated(pageEntryKey: PageEntryKey) {
    if (this.dataRef.value) {
      // just trigger the dataRef
      triggerRef(this.dataRef);
    } else {
      // set the dataRef.value initially
      this.dataRef.value = this.array;
    }

    this.pageEntryKey = pageEntryKey;
  }
}

const RESOLVED_PROMISE = Promise.resolve();

export class PageStore {
  private readonly backendPageSize: number;
  readonly pageSize: number;

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

  private readonly zeroedPageArray: Uint8Array;
  private readonly scratchPageArray: Uint8Array;

  constructor(private readonly backend: PageStoreBackend) {
    const backendPageSize = backend.pageSize;
    if (backendPageSize < MIN_PAGE_SIZE) {
      throw new Error("backend pageSize is too small");
    }
    if (backendPageSize > MAX_PAGE_SIZE) {
      throw new Error("backend pageSize is too large");
    }
    this.backendPageSize = backendPageSize;
    const pageSize = backendPageSize - PAGE_OVERHEAD;
    this.pageSize = pageSize;
    this.zeroedPageArray = new Uint8Array(pageSize);
    this.scratchPageArray = new Uint8Array(pageSize);
  }

  get loading(): boolean {
    return !!this.loadingPages.size;
  }

  private getIndexPage(): IndexPage | undefined {
    return this.backendPages.get(INDEX_PAGE_NUMBER)?.getAsIndexPage(this.backendPageSize);
  }

  private getTransactionIdOfPageGroupPage(pageGroupNumber: number, providedIndexPage?: IndexPage): number | undefined {
    const indexPage = providedIndexPage ?? this.getIndexPage();
    if (!indexPage) {
      return undefined;
    }
    const transactionId = indexPage.pageGroupNumberToTransactionId.get(pageGroupNumber) ?? 0;
    if (transactionId === 0 && indexPage.transactionIdsPageStoreTransactionId !== 0) {
      throw new Error("transactionIdsPageStoreTransactionId not yet implemented");
    }
    return transactionId;
  }

  private getPageGroupPage(pageGroupNumber: number, providedIndexPage?: IndexPage): PageGroupPage | undefined {
    const expectedTransactionId = this.getTransactionIdOfPageGroupPage(pageGroupNumber, providedIndexPage);
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

  private getBackendPageForPage(pageGroupPage: PageGroupPage, pageNumber: number): BackendPage | undefined {
    assertValidPageNumber(pageNumber);
    if (pageGroupPage.pageGroupNumber !== pageNumberToPageGroupNumber(pageNumber)) {
      throw new Error("invalid pageNumber for pageGroupPage: " + pageNumber);
    }

    const pageTransactionId = pageGroupPage.pageNumberToTransactionId.get(pageNumber);
    let backendPage = this.backendPages.get(pageNumber);
    if (pageTransactionId !== undefined) {
      const backendPageData = backendPage?.page?.data;
      if (!backendPageData) {
        return undefined;
      }
      const transactionId = readUint48FromDataView(uint8ArrayToDataView(backendPageData), 0);
      if (transactionId !== pageTransactionId) {
        return undefined;
      }
    } else if (!backendPage) {
      // there is no persisted page, just set the backendPage to what it should be
      backendPage = new BackendPage(undefined, pageNumber);
      this.backendPages.set(pageNumber, backendPage);
    }
    return backendPage;
  }

  private buildPageArray(pageNumber: number, array: Uint8Array): boolean {
    assertValidPageNumber(pageNumber);
    const indexPage = this.getIndexPage();
    if (!indexPage) {
      return false;
    }

    const pageGroupPage = this.getPageGroupPage(pageNumberToPageGroupNumber(pageNumber), indexPage);
    if (!pageGroupPage) {
      return false;
    }

    const backendPage = this.getBackendPageForPage(pageGroupPage, pageNumber);
    if (!backendPage) {
      return false;
    }

    const backendPageData = backendPage.page?.data;
    const baseArray = backendPageData ? backendPageData.subarray(PAGE_OVERHEAD) : this.zeroedPageArray;
    assert(array.length === baseArray.length);
    array.set(baseArray);

    // apply page group page patches
    pageGroupPage.pageNumberToPatches.get(pageNumber)?.forEach((patch) => patch.applyTo(array));

    // apply index page patches
    indexPage.pageNumberToPatches.get(pageNumber)?.forEach((patch) => patch.applyTo(array));

    return true;
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

  private buildPageEntryKey(pageNumber: number, indexPage: IndexPage): PageEntryKey | undefined {
    const pageGroupTransactionId = this.getTransactionIdOfPageGroupPage(
      pageNumberToPageGroupNumber(pageNumber),
      indexPage
    );
    if (pageGroupTransactionId === undefined) {
      return undefined;
    }
    return new PageEntryKey(
      indexPage.transactionId,
      pageGroupTransactionId,
      indexPage.pageNumberToPatches.get(pageNumber)
    );
  }

  private resetPageEntriesFromBackendPages(): void {
    if (this.loading) {
      throw new Error("loading");
    }

    const indexPage = this.getIndexPage();

    for (const [pageNumber, entry] of this.pageEntries.entries()) {
      if (indexPage !== undefined) {
        const pageEntryKey = this.buildPageEntryKey(pageNumber, indexPage);
        if (pageEntryKey) {
          if (entry.pageEntryKey && entry.pageEntryKey.equals(pageEntryKey)) {
            // nothing to do
            continue;
          } else {
            // the page might have changed
            const success = this.buildPageArray(pageNumber, entry.array);
            if (success) {
              entry.arrayUpdated(pageEntryKey);
              continue;
            }
          }
        }
      }
      // something is probably inconsistent, trigger a refresh for the page
      this.triggerLoad(pageNumber);
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

  getPage(pageNumber: number): Readonly<ShallowRef<Uint8Array | undefined>> {
    let pageEntry = this.pageEntries.get(pageNumber);
    if (!pageEntry) {
      pageEntry = new PageEntry(pageNumber, this.pageSize);
      this.pageEntries.set(pageNumber, pageEntry);
      if (!this.loading) {
        const success = this.buildPageArray(pageNumber, pageEntry.array);
        if (success) {
          const indexPage = this.getIndexPage();
          assert(indexPage);
          const pageEntryKey = this.buildPageEntryKey(pageNumber, indexPage);
          assert(pageEntryKey);
          pageEntry.arrayUpdated(pageEntryKey);
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

  private async commit(dirtyPageNumbers: Set<number>): Promise<boolean> {
    if (!this.transactionActive) {
      throw new Error("there is no transaction active");
    }

    const oldIndexPage = this.getIndexPage();
    if (!oldIndexPage) {
      throw new Error("index page not available");
    }
    const newIndexPage = new IndexPage(oldIndexPage);
    let changes = false;
    const oldPageData = this.scratchPageArray;
    for (const pageNumber of dirtyPageNumbers) {
      const entry = this.pageEntries.get(pageNumber);
      assert(entry);
      const data = entry.dataRef.value;
      assert(data);
      const success = this.buildPageArray(pageNumber, oldPageData);
      assert(success);
      const patches = Patch.createPatches(oldPageData, data, data.length);
      if (patches.length) {
        changes = true;
        newIndexPage.pageNumberToPatches.set(
          pageNumber,
          Patch.mergePatches([...(newIndexPage.pageNumberToPatches.get(pageNumber) ?? []), ...patches])
        );
      }
    }

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
        const oldPageGroupPage = this.getPageGroupPage(largestPageGroupNumber, oldIndexPage);
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
          const largestPageNumber = newPageGroupPage.determineLargestPage();

          if (largestPageNumber === undefined) {
            throw new Error("page group page too large, but no largest page");
          }
          const backendPage = this.getBackendPageForPage(oldPageGroupPage, largestPageNumber);
          if (!backendPage) {
            // this can actually happen, if the page was not loaded/modified...
            this.triggerLoad(largestPageNumber);
            return false;
          }

          const newBackendPageData = new Uint8Array(this.backendPageSize);
          writeUint48toDataView(uint8ArrayToDataView(newBackendPageData), 0, transactionId);
          newPageGroupPage.pageNumberToTransactionId.set(largestPageNumber, transactionId);

          const newPageArray = newBackendPageData.subarray(PAGE_OVERHEAD);
          const oldBackendPageData = backendPage.page?.data;
          if (oldBackendPageData) {
            newPageArray.set(oldBackendPageData.subarray(PAGE_OVERHEAD));
          }
          // apply patches (all relevant patches are in newPageGroupPage)
          newPageGroupPage.pageNumberToPatches.get(largestPageNumber)!.forEach((patch) => patch.applyTo(newPageArray));
          newPageGroupPage.pageNumberToPatches.delete(largestPageNumber);

          pagesToStore.push({
            pageNumber: largestPageNumber,
            data: newBackendPageData,
            previousVersion: backendPage.page?.version,
          });
        }

        const newPageGroupPageArray = new Uint8Array(this.backendPageSize);
        newPageGroupPage.serialize(newPageGroupPageArray);
        pagesToStore.push({
          pageNumber: pageGroupPageBackendPageNumber,
          data: newPageGroupPageArray,
          previousVersion: this.backendPages.get(pageGroupPageBackendPageNumber)?.page?.version,
        });
      }

      const newIndexPageArray = new Uint8Array(this.backendPageSize);
      newIndexPage.serialize(newIndexPageArray);
      pagesToStore.push({
        pageNumber: INDEX_PAGE_NUMBER,
        data: newIndexPageArray,
        previousVersion: this.backendPages.get(INDEX_PAGE_NUMBER)?.page?.version,
      });

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

    // the transaction is completed
    return true;
  }

  async runTransaction<T>(
    transactionFn: (pageAccess: PageAccessDuringTransaction) => T,
    retries?: number
  ): Promise<TransactionResult<T>> {
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
        const dirtyPageNumbers = new Set<number>();
        try {
          try {
            const get = (pageNumber: number): Uint8Array => {
              const result = this.getPage(pageNumber).value;
              if (!result) {
                throw new RetryRequiredError("page is not loaded");
              }
              return result;
            };
            resultValue = transactionFn({
              get,
              getForUpdate(pageNumber) {
                const result = get(pageNumber);
                dirtyPageNumbers.add(pageNumber);
                return result;
              },
            });
          } catch (e) {
            if (e instanceof RetryRequiredError) {
              // just retry
              continue;
            } else {
              // rethrow
              throw e;
            }
          }

          if (await this.commit(dirtyPageNumbers)) {
            result = {
              committed: true,
              resultValue,
            };
            break;
          }
        } finally {
          if (result.committed) {
            const indexPage = this.getIndexPage();
            assert(indexPage);
            // call arrayUpdated for the changed page entries
            for (const pageNumber of dirtyPageNumbers) {
              const pageEntry = this.pageEntries.get(pageNumber);
              assert(pageEntry);
              const pageEntryKey = this.buildPageEntryKey(pageNumber, indexPage);
              assert(pageEntryKey);
              pageEntry.arrayUpdated(pageEntryKey);
            }
          } else {
            // reset the modified pages
            for (const pageNumber of dirtyPageNumbers) {
              const pageEntry = this.pageEntries.get(pageNumber);
              assert(pageEntry);
              const success = this.buildPageArray(pageNumber, pageEntry.array);
              assert(success);
            }
          }
        }
      }
    } finally {
      this.transactionActive = false;
    }

    return result;
  }
}
