import { IndexPage } from "./internal/IndexPage";
import { Patch } from "./internal/Patch";
import { TreeCalc } from "./internal/TreeCalc";
import { readUint48FromDataView, writeUint48toDataView } from "./internal/util";
import { type PageAccessDuringTransaction } from "./PageAccessDuringTransaction";
import { type BackendPage, type BackendPageIdentifier, type PageStoreBackend } from "./PageStoreBackend";
import { uint8ArrayToDataView, assert } from "shared-util";

// require at least 4KB
const MIN_PAGE_SIZE = 1 << 12;
// max page size is 64KB to ensure that 16bit indexes are sufficient
const MAX_PAGE_SIZE = 1 << 16;
// page numbers are uint32
const MAX_PAGE_NUMBER_RAW = -1 >>> 0;

// a dummy page number that is just used for load handling
const DUMMY_INDEX_PAGE_NUMBER = -1;

/**
 * Internal error type to signal that a retry is required.
 */
class RetryRequiredError extends Error {
  constructor(message?: string) {
    super(message);
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
  if (pageNumber < 0 || pageNumber > MAX_PAGE_NUMBER_RAW) {
    throw new Error("invalid pageNumber: " + pageNumber);
  }
}

function assertValidPageSize(pageSize: number, pageTypePrefix: string) {
  if (pageSize < MIN_PAGE_SIZE) {
    throw new Error(pageTypePrefix + "page size is too small");
  }
  if (pageSize > MAX_PAGE_SIZE) {
    throw new Error(pageTypePrefix + "page size is too large");
  }
}

/**
 * Contains information that is necessary to check whether the page data might have changed. If two PageEntryKeys are
 * equal for a page, then the page cannot have changed, if they are not, then it might have changed.
 */
class PageEntryKey {
  constructor(readonly pageTransactionId: number, readonly patches: Patch[] | undefined) {}

  equals(other: PageEntryKey): boolean {
    return this.pageTransactionId === other.pageTransactionId && Patch.patchesEqual(this.patches, other.patches);
  }
}

class PageEntry {
  backendPage?: BackendPage;

  readonly array: Uint8Array;

  /** Is set if the array is actually initialized from data from the backend pages. */
  pageEntryKey: PageEntryKey | undefined = undefined;

  readonly callbacks = new Set<() => void>();

  constructor(readonly pageNumber: number, pageSize: number) {
    assertValidPageNumber(pageNumber);
    this.array = new Uint8Array(pageSize);
  }

  arrayUpdated(pageEntryKey: PageEntryKey) {
    this.pageEntryKey = pageEntryKey;

    // trigger all the callbacks
    this.callbacks.forEach((callback) => callback());
  }

  getArrayIfLoaded(): Uint8Array | undefined {
    return this.pageEntryKey ? this.array : undefined;
  }
}

export type PageReadsRecorder = <R>(pageReader: (getPage: (pageNumber: number) => Uint8Array | undefined) => R) => R;

const RESOLVED_PROMISE = Promise.resolve();

export class PageStore {
  private readonly treeCalc: TreeCalc;

  private indexPage?: IndexPage;

  // TODO: use "MapWithWeakRefValues" to allow garbage collection...
  private readonly pageEntries = new Map<number, PageEntry>();

  /** Caches the transaction ids of all pages, is invalidated/cleared whenever a new index page is loaded. */
  private readonly transactionIdCache = new Map<number, number>();

  /** Pages that will be loaded in the next microtask. */
  private readonly loadTriggeredPages = new Set<number>();
  /** All pages that are currently in "loading". */
  private readonly loadingPages = new Set<number>();

  private loadingFinishedPromise?: Promise<void>;
  private loadingFinishedResolve?: () => void;

  private transactionActive = false;

  private readonly zeroedPageArray: Uint8Array;
  private readonly scratchPageArray: Uint8Array;

  constructor(
    private readonly backend: PageStoreBackend,
    /**
     * The exact size of all pages. This cannot be changed (for a specific backend) after the first transaction was
     * committed.
     */
    readonly pageSize: number,
    /**
     * The maximum size of the index page. This can generally be changed, it is only used when writing new
     * transactions. When an existing index page is read, then it is allowed to be larger.
     */
    readonly maxIndexPageSize: number
  ) {
    assertValidPageSize(pageSize, "");
    assertValidPageSize(maxIndexPageSize, "index ");
    const backendMaxPageSize = backend.maxPageSize;
    if (pageSize > backendMaxPageSize) {
      throw new Error("page size is too large for backend");
    }
    if (maxIndexPageSize > backendMaxPageSize) {
      throw new Error("index page size is too large for backend");
    }

    this.treeCalc = new TreeCalc(pageSize, 6, MAX_PAGE_NUMBER_RAW);

    this.zeroedPageArray = new Uint8Array(pageSize);
    this.scratchPageArray = new Uint8Array(pageSize);
  }

  get loading(): boolean {
    return !!this.loadingPages.size;
  }

  get maxPageNumber(): number {
    return this.treeCalc.maxPageNumber;
  }

  private assertValidUsablePageNumber(pageNumber: number): void {
    if (pageNumber > this.treeCalc.maxPageNumber) {
      throw new Error("pageNumber cannot be used: " + pageNumber);
    }
  }

  private getTransactionIdOfPage(pageNumber: number): number | undefined {
    const cachedResult = this.transactionIdCache.get(pageNumber);
    if (cachedResult !== undefined) {
      return cachedResult;
    }
    if (!this.indexPage) {
      return undefined;
    }
    if (pageNumber > this.treeCalc.maxPageNumber) {
      // for these pages the pageNumber must be in transactionIdCache, otherwise just return undefined
      // TODO it should be possible to modify TreeCalc to also handle these pages...
      return undefined;
    }

    let transactionId = this.indexPage.transactionTreeRootTransactionId;
    for (const pathElement of this.treeCalc.getPath(pageNumber)) {
      const cachedTransactionId = this.transactionIdCache.get(pathElement.pageNumber);
      if (cachedTransactionId === undefined) {
        // populate the cache before calling getPageEntry() because it might call this method again
        this.transactionIdCache.set(pathElement.pageNumber, transactionId);
      } else {
        assert(transactionId === cachedTransactionId);
      }

      const pageEntry = this.getPageEntry(pathElement.pageNumber);
      if (cachedTransactionId === undefined) {
        // these pages are not handled in resetPageEntriesFromBackendPages(), so we need to do it here
        this.resetPageEntryFromBackendPage(pageEntry);
      }
      const pageArray = pageEntry.getArrayIfLoaded();
      if (!pageArray) {
        return undefined;
      }
      // TODO maybe find away to avoid creating the DataView here
      const view = uint8ArrayToDataView(pageArray);
      transactionId = readUint48FromDataView(view, pathElement.offset);
    }

    this.transactionIdCache.set(pageNumber, transactionId);
    return transactionId;
  }

  private buildPageArray(pageEntry: PageEntry, array: Uint8Array): boolean {
    if (!this.indexPage) {
      return false;
    }

    const transactionId = this.getTransactionIdOfPage(pageEntry.pageNumber);
    if (transactionId === undefined) {
      return false;
    }
    let baseArray: Uint8Array;
    if (transactionId === 0) {
      baseArray = this.zeroedPageArray;
    } else {
      const backendPage = pageEntry.backendPage;
      if (!backendPage || backendPage.identifier.transactionId !== transactionId) {
        return false;
      }
      baseArray = backendPage.data;
    }

    assert(array.length === baseArray.length);
    array.set(baseArray);

    // apply index page patches
    this.indexPage.pageNumberToPatches.get(pageEntry.pageNumber)?.forEach((patch) => patch.applyTo(array));

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

  private buildPageEntryKey(pageNumber: number): PageEntryKey | undefined {
    const transactionId = this.getTransactionIdOfPage(pageNumber);
    if (transactionId === undefined || !this.indexPage) {
      return undefined;
    }
    return new PageEntryKey(transactionId, this.indexPage.pageNumberToPatches.get(pageNumber));
  }

  private resetPageEntryFromBackendPage(pageEntry: PageEntry): void {
    const pageEntryKey = this.buildPageEntryKey(pageEntry.pageNumber);
    if (pageEntryKey) {
      if (pageEntry.pageEntryKey && pageEntry.pageEntryKey.equals(pageEntryKey)) {
        // nothing to do
        return;
      } else {
        // the page might have changed
        const success = this.buildPageArray(pageEntry, pageEntry.array);
        if (success) {
          pageEntry.arrayUpdated(pageEntryKey);
          return;
        }
      }
    }
    // something is probably inconsistent, trigger a refresh for the page
    pageEntry.pageEntryKey = undefined;
    this.triggerLoad(pageEntry.pageNumber);
  }

  private resetPageEntriesFromBackendPages(): void {
    if (this.loading) {
      throw new Error("loading");
    }

    const maxPageNumber = this.maxPageNumber;
    for (const pageEntry of this.pageEntries.values()) {
      // only handle "normal" pages here, the other ones will be handled in getTransactionIdOfPage()
      if (pageEntry.pageNumber <= maxPageNumber) {
        this.resetPageEntryFromBackendPage(pageEntry);
      }
    }
  }

  private async processLoad(pageNumbers: number[]): Promise<void> {
    const backendPageIdentifiers: BackendPageIdentifier[] = [];
    for (const pageNumber of pageNumbers) {
      if (pageNumber >= 0) {
        const transactionId = this.getTransactionIdOfPage(pageNumber);
        if (transactionId !== undefined && transactionId > 0) {
          backendPageIdentifiers.push({ pageNumber, transactionId });
        }
      }
    }
    // always include the index page for now
    // TODO: error handling?!
    const readResult = await this.backend.readPages(true, backendPageIdentifiers);

    if (readResult.indexPage && this.indexPage?.transactionId !== readResult.indexPage.transactionId) {
      // we got a new index page
      this.indexPage = IndexPage.deserialize(
        readResult.indexPage.transactionId,
        readResult.indexPage.data,
        this.pageSize
      );
      this.transactionIdCache.clear();
    }

    for (const backendPage of readResult.pages) {
      const pageNumber = backendPage.identifier.pageNumber;
      const pageEntry = this.pageEntries.get(pageNumber);
      // the pageEntry should always exist, but still just skip it if not
      if (pageEntry) {
        pageEntry.backendPage = backendPage;
      }
      this.loadingPages.delete(pageNumber);
    }

    // remove all requested pageNumbers from loadingPages, even if they were not actually loaded
    // if they are still needed a new load will be triggered by resetPageEntriesFromBackendPages()
    for (const pageNumber of pageNumbers) {
      this.loadingPages.delete(pageNumber);
    }

    if (!this.loadingPages.size) {
      // all current loads are finished, update all page entries now
      assert(!this.loadTriggeredPages.size);

      this.resetPageEntriesFromBackendPages();

      // we might be loading again here (resetPageEntriesFromBackendPages can trigger new loads)
      if (!this.loading) {
        this.loadingFinishedResolve?.();
        this.loadingFinishedResolve = undefined;
        this.loadingFinishedPromise = undefined;
      }
    }
  }

  private triggerLoad(pageNumber: number): void {
    if (this.loadingPages.has(pageNumber)) {
      // this page is already loading
      return;
    }
    const needNewTask = !this.loadTriggeredPages.size;
    this.loadTriggeredPages.add(pageNumber);
    this.loadingPages.add(pageNumber);

    if (needNewTask) {
      queueMicrotask(() => {
        const pageNumbers = [...this.loadTriggeredPages];
        this.loadTriggeredPages.clear();
        void this.processLoad(pageNumbers);
      });
    }
  }

  private getPageEntry(pageNumber: number): PageEntry {
    let pageEntry = this.pageEntries.get(pageNumber);
    if (!pageEntry) {
      pageEntry = new PageEntry(pageNumber, this.pageSize);
      this.pageEntries.set(pageNumber, pageEntry);
      if (!this.loading) {
        const success = this.buildPageArray(pageEntry, pageEntry.array);
        if (success) {
          const pageEntryKey = this.buildPageEntryKey(pageNumber);
          assert(pageEntryKey);
          pageEntry.arrayUpdated(pageEntryKey);
        }
      }
      if (!pageEntry.pageEntryKey) {
        this.triggerLoad(pageNumber);
      }
    }
    return pageEntry;
  }

  getPage(pageNumber: number): Uint8Array | undefined {
    this.assertValidUsablePageNumber(pageNumber);
    return this.getPageEntry(pageNumber).getArrayIfLoaded();
  }

  /**
   * This method returns a PageReadsRecorder, a function that allows doing one or more page gets/reads in a combined
   * operation while recording exactly which pages were read. If any of those pages later changes, then the given
   * pageChangeCallback is called.
   *
   * Each call to the PageReadsRecorder "resets" the list of recorded pages, so the pageChangeCallback is only called
   * if pages that were read in the last invocation of the function change.
   *
   * To "deregister" the callback and avoid memory leaks just call the PageReadsRecorder once without doing any page
   * reads.
   *
   * @param pageChangeCallback
   *   the callback that will be called if any page, that was read in the last PageReadsRecorder invocation, changes
   */
  getPageReadsRecorder(pageChangeCallback: () => void): PageReadsRecorder {
    // wrap the callback so we have a distinct identity (in case the caller reuses the callback function)
    const wrappedCallBack: () => void = () => pageChangeCallback();
    const currentPageEntries = new Map<number, { readonly entry: PageEntry; toggle: boolean }>();
    // this is used to find no longer needed entries without needing a separate map copy every time
    let currentToggle = false;
    let active = false;
    return (pageReader) => {
      assert(!active, "already active");
      active = true;
      currentToggle = !currentToggle;

      try {
        const getPage = (pageNumber: number) => {
          this.assertValidUsablePageNumber(pageNumber);

          let entryWithToggle = currentPageEntries.get(pageNumber);
          if (entryWithToggle) {
            entryWithToggle.toggle = currentToggle;
          } else {
            const entry = this.getPageEntry(pageNumber);
            entryWithToggle = { entry, toggle: currentToggle };
            currentPageEntries.set(pageNumber, entryWithToggle);
            entry.callbacks.add(wrappedCallBack);
          }
          return entryWithToggle.entry.getArrayIfLoaded();
        };

        return pageReader(getPage);
      } finally {
        // remove pages that were not toggled/read
        currentPageEntries.forEach((entryWithToggle, pageNumber) => {
          if (entryWithToggle.toggle != currentToggle) {
            currentPageEntries.delete(pageNumber);
            // also remove the callback
            entryWithToggle.entry.callbacks.delete(wrappedCallBack);
          }
        });
        active = false;
      }
    };
  }

  /**
   * Triggers a refresh of all pages, to make sure that they are up to date with the pages in the backend.
   */
  refresh(): void {
    // trigger a load with the dummy index page number, if there are changes it will trigger further loads
    this.triggerLoad(DUMMY_INDEX_PAGE_NUMBER);
  }

  private async commit(updatedPages: Map<number, Uint8Array>, triedTransactionIds: Set<number>): Promise<boolean> {
    if (!this.transactionActive) {
      throw new Error("there is no transaction active");
    }

    const oldIndexPage = this.indexPage;
    if (!oldIndexPage) {
      throw new Error("index page not available");
    }
    const newIndexPage = new IndexPage(oldIndexPage);
    let changes = false;
    const oldPageData = this.scratchPageArray;
    for (const pageNumber of dirtyPageNumbers) {
      const entry = this.pageEntries.get(pageNumber);
      assert(entry);
      const success = this.buildPageArray(entry, oldPageData);
      assert(success);
      const data = entry.array;
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
      let transactionId = oldIndexPage.transactionId + 1;
      while (triedTransactionIds.has(transactionId)) {
        // avoid trying a previous transaction id again
        ++transactionId;
      }
      triedTransactionIds.add(transactionId);
      const pagesToStore: BackendPageToStore[] = [];

      // push changes down to the individual pages as necessary
      while (newIndexPage.serializedLength > this.maxIndexPageSize) {
        const largestPageGroupNumber = newIndexPage.determineLargestPageGroup();

        if (largestPageGroupNumber === undefined) {
          throw new Error("index page too large, but no largest page group");
        }

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
      // TODO maybe automatically "serialize" the transactions (by just waiting until the previous one is finished)
      throw new Error("there is already an active transaction");
    }
    this.transactionActive = true;

    let result: TransactionResult<T> = {
      committed: false,
    };

    try {
      const triedTransactionIds: Set<number> = new Set();
      for (let retry = 0; retries === undefined || retry <= retries; ++retry) {
        if (retry > 0) {
          // we are in a retry, so refresh first
          this.refresh();
          await this.loadingFinished();

          // TODO maybe sleep a bit here if the transactionId is still the same as before to avoid concurrent retries?!
        }

        let resultValue: T;
        const updatedPages = new Map<number, Uint8Array>();
        try {
          try {
            const get = (pageNumber: number): Uint8Array => {
              const updatedPage = updatedPages.get(pageNumber);
              if (updatedPage) {
                return updatedPage;
              }
              const result = this.getPage(pageNumber);
              if (!result) {
                throw new RetryRequiredError("page is not loaded");
              }
              return result;
            };
            resultValue = transactionFn({
              get,
              getForUpdate(pageNumber) {
                const updatedPage = updatedPages.get(pageNumber);
                if (updatedPage) {
                  return updatedPage;
                }
                // first call for this pageNumber, create a copy and put it into updatedPages
                const result = Uint8Array.from(get(pageNumber));
                updatedPages.set(pageNumber, result);
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

          if (await this.commit(updatedPages, triedTransactionIds)) {
            result = {
              committed: true,
              resultValue,
            };
            break;
          }
        } finally {
          if (result.committed) {
            const indexPage = this.indexPage;
            assert(indexPage);
            // call arrayUpdated for the changed page entries
            // TODO
            for (const pageNumber of dirtyPageNumbers) {
              const pageEntry = this.pageEntries.get(pageNumber);
              assert(pageEntry);
              const pageEntryKey = this.buildPageEntryKey(pageNumber);
              assert(pageEntryKey);
              pageEntry.arrayUpdated(pageEntryKey);
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
