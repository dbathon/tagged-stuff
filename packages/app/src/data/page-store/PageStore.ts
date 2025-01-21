import { IndexPage } from "./internal/IndexPage";
import { Patch } from "./internal/Patch";
import { TreeCalc } from "./internal/TreeCalc";
import { readUint48FromDataView, writeUint48toDataView } from "./internal/util";
import { type PageAccessDuringTransaction } from "./PageAccessDuringTransaction";
import {
  type BackendIndexPage,
  type BackendPage,
  type BackendPageIdentifier,
  type BackendReadResult,
  type PageStoreBackend,
} from "./PageStoreBackend";
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

type CommitData = {
  indexPage: BackendIndexPage;
  pages: BackendPage[];
};

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
  constructor(
    readonly pageTransactionId: number,
    readonly patches: Patch[] | undefined,
  ) {}

  equals(other: PageEntryKey): boolean {
    return this.pageTransactionId === other.pageTransactionId && Patch.patchesEqual(this.patches, other.patches);
  }
}

class PageEntry {
  backendPage?: BackendPage;

  readonly array: Uint8Array;

  /** Is set if the array is actually initialized from data from the backend pages. */
  pageEntryKey?: PageEntryKey = undefined;

  /** Is used as a cache for the expected transactionId of the backendPage */
  transactionId?: number = undefined;

  readonly callbacks = new Set<() => void>();

  constructor(
    readonly pageNumber: number,
    pageSize: number,
  ) {
    assertValidPageNumber(pageNumber);
    this.array = new Uint8Array(pageSize);
  }

  getArrayIfLoaded(): Uint8Array | undefined {
    const transactionId = this.transactionId;
    return this.pageEntryKey &&
      transactionId !== undefined &&
      (transactionId === 0 || transactionId === this.backendPage?.identifier.transactionId)
      ? this.array
      : undefined;
  }
}

export type PageReadsRecorder = <R>(pageReader: (getPage: (pageNumber: number) => Uint8Array | undefined) => R) => R;

const RESOLVED_PROMISE = Promise.resolve();

export class PageStore {
  private readonly treeCalc: TreeCalc;

  private indexPage?: IndexPage;

  // TODO: use "MapWithWeakRefValues" to allow garbage collection...
  private readonly pageEntries = new Map<number, PageEntry>();

  /** Pages that will be loaded in the next microtask. */
  private readonly loadTriggeredPages = new Set<number>();
  /** All pages that are currently in "loading". */
  private readonly loadingPages = new Set<number>();

  private loadingFinishedPromise?: Promise<void>;
  private loadingFinishedResolve?: () => void;

  private transactionActive = false;

  private readonly zeroedPageArray: Uint8Array;

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
    readonly maxIndexPageSize: number,
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
  }

  get loading(): boolean {
    return !!this.loadingPages.size;
  }

  get maxPageNumber(): number {
    return this.treeCalc.maxNormalPageNumber;
  }

  private assertValidUsablePageNumber(pageNumber: number): void {
    if (pageNumber > this.treeCalc.maxNormalPageNumber) {
      throw new Error("pageNumber cannot be used: " + pageNumber);
    }
  }

  private getTransactionIdOfPage(pageEntry: PageEntry): number | undefined {
    let result = pageEntry.transactionId;

    if (result === undefined) {
      const transactionIdLocation = this.treeCalc.getTransactionIdLocation(pageEntry.pageNumber);
      if (!transactionIdLocation) {
        result = this.indexPage?.transactionTreeRootTransactionId;
      } else {
        const pageArray = this.getPageEntry(transactionIdLocation.pageNumber).getArrayIfLoaded();
        if (pageArray) {
          // TODO maybe find away to avoid creating the DataView here
          result = readUint48FromDataView(uint8ArrayToDataView(pageArray), transactionIdLocation.offset);
        }
      }
    }
    if (result !== undefined) {
      pageEntry.transactionId = result;
    }

    return result;
  }

  private getBaseArray(pageEntry: PageEntry): Uint8Array | undefined {
    const transactionId = this.getTransactionIdOfPage(pageEntry);
    if (transactionId === undefined) {
      return undefined;
    }
    if (transactionId === 0) {
      return this.zeroedPageArray;
    } else {
      const backendPage = pageEntry.backendPage;
      if (!backendPage || backendPage.identifier.transactionId !== transactionId) {
        return undefined;
      }
      return backendPage.data;
    }
  }

  private buildPageArray(pageEntry: PageEntry, array: Uint8Array): boolean {
    if (!this.indexPage) {
      return false;
    }

    const baseArray = this.getBaseArray(pageEntry);
    if (!baseArray) {
      return false;
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

  private buildPageEntryKey(pageEntry: PageEntry): PageEntryKey | undefined {
    const transactionId = this.getTransactionIdOfPage(pageEntry);
    if (transactionId === undefined || !this.indexPage) {
      return undefined;
    }
    return new PageEntryKey(transactionId, this.indexPage.pageNumberToPatches.get(pageEntry.pageNumber));
  }

  private resetPageEntriesFromBackendPages(): void {
    if (this.loading) {
      throw new Error("loading");
    }

    // we have to process the page entries in a specific order:
    // first the transaction tree pages from top to bottom and then the "normal" pages (in any order)
    // this is necessary for getTransactionIdOfPage() to work properly
    const maxPageNumber = this.maxPageNumber;
    const normalPageEntries: PageEntry[] = [];
    const treePageEntries: PageEntry[] = [];
    for (const pageEntry of this.pageEntries.values()) {
      (pageEntry.pageNumber <= maxPageNumber ? normalPageEntries : treePageEntries).push(pageEntry);
    }

    treePageEntries.sort((a, b) => a.pageNumber - b.pageNumber);

    // collect all relevant callbacks and call them all at the end (each one only once)
    const callbacks = new Set<() => void>();

    for (const pageEntry of [...treePageEntries, ...normalPageEntries]) {
      const pageEntryKey = this.buildPageEntryKey(pageEntry);
      if (pageEntryKey) {
        if (pageEntry.pageEntryKey?.equals(pageEntryKey)) {
          // nothing really to do, but still use the new pageEntryKey to allow GC of old index page patches
          pageEntry.pageEntryKey = pageEntryKey;
          continue;
        } else {
          // the page might have changed
          const success = this.buildPageArray(pageEntry, pageEntry.array);
          if (success) {
            pageEntry.pageEntryKey = pageEntryKey;
            pageEntry.callbacks.forEach((callback) => callbacks.add(callback));
            continue;
          }
        }
      }
      // something is probably inconsistent, trigger a refresh for the page
      pageEntry.pageEntryKey = undefined;
      this.triggerLoad(pageEntry.pageNumber);
    }

    callbacks.forEach((callback) => callback());
  }

  private async processLoad(pageNumbers: number[]): Promise<void> {
    const backendPageIdentifiers: BackendPageIdentifier[] = [];
    for (const pageNumber of pageNumbers) {
      if (pageNumber >= 0) {
        const transactionId = this.getTransactionIdOfPage(this.getPageEntry(pageNumber));
        if (transactionId !== undefined && transactionId > 0) {
          backendPageIdentifiers.push({ pageNumber, transactionId });
        }
      }
    }
    // always include the index page for now
    // TODO: error handling?!
    const readResult = await this.backend.readPages(true, backendPageIdentifiers);

    // remove all requested pageNumbers from loadingPages, even if they were not actually loaded
    // if they are still needed a new load will be triggered by resetPageEntriesFromBackendPages()
    for (const pageNumber of pageNumbers) {
      this.loadingPages.delete(pageNumber);
    }

    this.applyReadResult(readResult);
  }

  private applyReadResult(readResult: BackendReadResult): void {
    if (readResult.indexPage && this.indexPage?.transactionId !== readResult.indexPage.transactionId) {
      // we got a new index page
      this.indexPage = IndexPage.deserialize(
        readResult.indexPage.transactionId,
        readResult.indexPage.data,
        this.pageSize,
      );
      // reset all the cached transaction ids
      this.pageEntries.forEach((entry) => (entry.transactionId = undefined));
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

    if (!this.loadingPages.size) {
      // all current loads are finished, update all page entries now
      this.loadTriggeredPages.clear();

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
        if (pageNumbers.length) {
          void this.processLoad(pageNumbers);
        }
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
          const pageEntryKey = this.buildPageEntryKey(pageEntry);
          assert(pageEntryKey);
          pageEntry.pageEntryKey = pageEntryKey;
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

  private buildCommitData(updatedPages: Map<number, Uint8Array>, triedTransactionIds: Set<number>): CommitData | false {
    if (!updatedPages.size) {
      return false;
    }
    const oldIndexPage = this.indexPage;
    // if there were any changes, then the index page must be available
    assert(oldIndexPage);

    let changes = false;
    const newPatches = new Map(oldIndexPage.pageNumberToPatches);
    updatedPages.forEach((array, pageNumber) => {
      const baseArray = this.getBaseArray(this.getPageEntry(pageNumber));
      // the baseArray needs to be available if there are changes
      assert(baseArray);
      const newPatchesForPage = Patch.createPatches(baseArray, array, array.length);
      if (!changes) {
        const oldPatchesForPage = newPatches.get(pageNumber);
        if (Patch.patchesEqual(oldPatchesForPage, newPatchesForPage.length ? newPatchesForPage : undefined)) {
          // no actual changes
          return;
        }
      }
      changes = true;
      if (newPatchesForPage.length) {
        newPatches.set(pageNumber, newPatchesForPage);
      } else {
        newPatches.delete(pageNumber);
      }
    });

    if (!changes) {
      return false;
    }

    // at this point newPatches contains existing patches for unchanged pages and new patches for updated pages

    let transactionId = oldIndexPage.transactionId + 1;
    while (triedTransactionIds.has(transactionId)) {
      // avoid trying a previous transaction id again
      ++transactionId;
    }

    let transactionTreeRootTransactionId = oldIndexPage.transactionTreeRootTransactionId;
    const backendPages: BackendPage[] = [];

    /**
     * This is mainly needed for the transaction tree pages, because they can be modified below (when a transaction id
     * is updated), but they can also be materialized to a backend page. And both of those things can happen in any
     * order.
     */
    type PageState = {
      readonly baseArray: Uint8Array;
      readonly array: Uint8Array;
      backendPage?: BackendPage;
    };
    const pageStates = new Map<number, PageState>();
    const getPageState = (pageNumber: number): PageState => {
      let result = pageStates.get(pageNumber);
      if (!result) {
        const baseArray = this.getBaseArray(this.getPageEntry(pageNumber));
        if (!baseArray) {
          // this pageNumber is not loaded, so we unfortunately need to wait until it is loaded and retry...
          throw new RetryRequiredError();
        }
        const array = Uint8Array.from(baseArray);
        newPatches.get(pageNumber)?.forEach((patch) => patch.applyTo(array));
        result = { baseArray, array };
        pageStates.set(pageNumber, result);
      }
      return result;
    };

    while (true) {
      const newIndexPage = new IndexPage(transactionId, this.pageSize, transactionTreeRootTransactionId, newPatches);

      if (newIndexPage.serializedLength <= this.maxIndexPageSize) {
        triedTransactionIds.add(transactionId);
        return {
          indexPage: {
            transactionId,
            data: newIndexPage.serialize(),
          },
          pages: backendPages,
        };
      }

      // the patches are too big, find the page with the largest patches and write a new backend page
      type LargePatchesInfo = {
        readonly pageNumber: number;
        readonly size: number;
      };
      let largestPatches: LargePatchesInfo | undefined = undefined;
      for (const [pageNumber, patches] of newPatches.entries()) {
        let size = 0;
        patches.forEach((patch) => (size += patch.serializedLength));
        if (largestPatches === undefined || size > largestPatches.size) {
          largestPatches = { pageNumber, size };
        }
      }
      assert(largestPatches);

      const pageState = getPageState(largestPatches.pageNumber);
      assert(!pageState.backendPage);
      newPatches.delete(largestPatches.pageNumber);
      const newBackendPage: BackendPage = {
        identifier: {
          pageNumber: largestPatches.pageNumber,
          transactionId,
        },
        data: pageState.array,
      };
      backendPages.push(newBackendPage);
      pageState.backendPage = newBackendPage;

      // update the transaction id in the transaction tree
      const transactionIdLocation = this.treeCalc.getTransactionIdLocation(largestPatches.pageNumber);
      if (!transactionIdLocation) {
        // we materialized the root of the transaction tree
        transactionTreeRootTransactionId = transactionId;
      } else {
        const transactionTreePageState = getPageState(transactionIdLocation.pageNumber);

        writeUint48toDataView(
          uint8ArrayToDataView(transactionTreePageState.array),
          transactionIdLocation.offset,
          transactionId,
        );
        if (!transactionTreePageState.backendPage) {
          // create new patches
          newPatches.set(
            transactionIdLocation.pageNumber,
            Patch.createPatches(
              transactionTreePageState.baseArray,
              transactionTreePageState.array,
              transactionTreePageState.array.length,
            ),
          );
        } else {
          // if backendPage is set, then array is the array of the new backend page and has been modified in place
        }
      }
    }
  }

  async runTransaction<T>(
    transactionFn: (pageAccess: PageAccessDuringTransaction) => T,
    retries?: number,
  ): Promise<TransactionResult<T>> {
    if (this.loading) {
      await this.loadingFinished();
    }

    if (this.transactionActive) {
      // TODO maybe automatically "serialize" the transactions (by just waiting until the previous one is finished)
      throw new Error("there is already an active transaction");
    }
    this.transactionActive = true;

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
        let commitData: CommitData | false;
        try {
          const updatedPages = new Map<number, Uint8Array>();
          const get = (pageNumber: number): Uint8Array => {
            const updatedPage = updatedPages.get(pageNumber);
            if (updatedPage) {
              return updatedPage;
            }
            const result = this.getPage(pageNumber);
            if (!result) {
              throw new RetryRequiredError();
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

          commitData = this.buildCommitData(updatedPages, triedTransactionIds);
        } catch (e) {
          if (e instanceof RetryRequiredError) {
            // just retry
            continue;
          } else {
            // rethrow
            throw e;
          }
        }

        if (!commitData) {
          // if commitData is undefined, then there are no changes and nothing needs to be done
        } else {
          const indexPage = this.indexPage;
          // if there were any changes, then the index page must be available
          assert(indexPage);

          const success = await this.backend.writePages(
            commitData.indexPage,
            indexPage.transactionId,
            commitData.pages,
          );
          if (success) {
            // just apply the commit data as if it was a "read result"
            this.applyReadResult(commitData);
          } else {
            // not committed, retry
            continue;
          }
        }
        return { committed: true, resultValue };
      }
    } finally {
      this.transactionActive = false;
    }

    return { committed: false };
  }
}
