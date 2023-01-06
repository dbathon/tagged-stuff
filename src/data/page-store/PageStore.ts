import { shallowReadonly, shallowRef, ShallowRef } from "vue";
import { BackendPageAndVersion, BackendPageToStore, PageStoreBackend } from "./PageStoreBackend";

// require at least 4KB
const MIN_PAGE_SIZE = 1 << 12;
// max page size is 64KB to ensure that 16bit indexes are sufficient
const MAX_PAGE_SIZE = 1 << 16;

/**
 * A simple immutable wrapper around an ArrayBuffer that also provides lazily constructed DataView and UInt8Array
 * views.
 */
export class PageData {
  private _dataView?: DataView;
  private _array?: Uint8Array;

  constructor(readonly buffer: ArrayBuffer) {}

  get dataView(): DataView {
    let result = this._dataView;
    if (!result) {
      result = this._dataView = new DataView(this.buffer);
    }
    return result;
  }

  get array(): Uint8Array {
    let result = this._array;
    if (!result) {
      result = this._array = new Uint8Array(this.buffer);
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

class PageEntry {
  readonly dataRef: ShallowRef<PageData | undefined>;
  readonly readonlyDataRef: Readonly<ShallowRef<PageData | undefined>>;

  backendPageVersion?: number;
  dirty = false;

  constructor(readonly pageNumber: number) {
    if (pageNumber < 0) {
      throw new Error("invalid pageNumber");
    }
    this.dataRef = shallowRef();
    this.readonlyDataRef = shallowReadonly(this.dataRef);
  }
}

const RESOLVED_PROMISE = Promise.resolve();

export class PageStore {
  // TODO: use "MapWithWeakRefValues" to allow garbage collection...
  private readonly backendPages = new Map<number, BackendPageAndVersion>();
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
  private optimisticLockError = false;

  constructor(private readonly backend: PageStoreBackend) {
    const pageSize = backend.pageSize;
    if (pageSize < MIN_PAGE_SIZE) {
      throw new Error("backend pageSize is too small");
    }
    if (pageSize > MAX_PAGE_SIZE) {
      throw new Error("backend pageSize is too large");
    }
  }

  get pageSize(): number {
    return this.backend.pageSize;
  }

  get loading(): boolean {
    return !!this.loadingPages.size;
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

    this.pageEntries.forEach((entry, pageNumber) => {
      const backendPage = this.backendPages.get(pageNumber);
      if (backendPage?.version !== entry.backendPageVersion || entry.dirty || entry.dataRef.value === undefined) {
        if (this.transactionActive && entry.dataRef.value) {
          // we are in a transaction and a page that was already loaded was updated => fail
          this.optimisticLockError = true;
        }

        // if the page does not exist, just initialize it with zero bytes
        const newBuffer = backendPage ? backendPage.data.slice(0) : new ArrayBuffer(this.pageSize);
        entry.dataRef.value = new PageData(newBuffer);
        entry.backendPageVersion = backendPage?.version;
        entry.dirty = false;
      }
    });
  }

  private async processLoad(backendPageNumbers: number[]): Promise<void> {
    // TODO: error handling?!
    const loadResults = await this.backend.loadPages(backendPageNumbers);
    backendPageNumbers.forEach((pageNumber, index) => {
      const loadResult = loadResults[index];
      if (loadResult) {
        this.backendPages.set(pageNumber, loadResult);
      }
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

      this.loadingFinishedResolve?.();
      this.loadingFinishedResolve = undefined;
      this.loadingFinishedPromise = undefined;
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
  }

  getPage(pageNumber: number): Readonly<ShallowRef<PageData | undefined>> {
    let pageEntry = this.pageEntries.get(pageNumber);
    if (!pageEntry) {
      pageEntry = new PageEntry(pageNumber);
      this.pageEntries.set(pageNumber, pageEntry);
      this.triggerLoad(pageNumber);
    }
    return pageEntry.readonlyDataRef;
  }

  /**
   * Triggers a refresh of all pages, to make sure that they are up to date with the pages in the backend.
   */
  refresh(): void {
    // for now just trigger a load of all pages...
    this.pageEntries.forEach((entry) => {
      this.triggerLoad(entry.pageNumber);
    });
  }

  /**
   * This method needs to be called before modifying a page during a transaction.
   *
   * Calling this method is only allowed during a transaction.
   */
  getPageDataForUpdate(pageNumber: number): PageData {
    if (!this.transactionActive) {
      throw new Error("no transaction active");
    }
    const entry = this.pageEntries.get(pageNumber);
    const result = entry?.dataRef.value;
    if (!result) {
      throw new Error("page is not loaded");
    }

    entry.dirty = true;
    return result;
  }

  private async commit(): Promise<boolean> {
    if (!this.transactionActive) {
      throw new Error("there is no transaction active");
    }
    if (this.optimisticLockError) {
      return false;
    }

    const pagesToStore: BackendPageToStore[] = [];
    this.pageEntries.forEach((entry, pageNumber) => {
      const data = entry.dataRef.value;
      if (data && entry.dirty) {
        // TODO: maybe check whether the page data actually changed
        pagesToStore.push({
          pageNumber,
          data: data.buffer.slice(0),
          previousVersion: entry.backendPageVersion,
        });
      }
    });

    if (pagesToStore.length) {
      const storeResult = await this.backend.storePages(pagesToStore);
      if (!storeResult) {
        return false;
      }

      // update backendPages
      storeResult.forEach((newVersion, index) => {
        const pageToStore = pagesToStore[index];
        const backEndPage = this.backendPages.get(pageToStore.pageNumber);
        if (backEndPage) {
          backEndPage.data = pageToStore.data;
          backEndPage.version = newVersion;
        } else {
          this.backendPages.set(pageToStore.pageNumber, {
            data: pageToStore.data,
            version: newVersion,
          });
        }
      });
    }

    // the transaction is completed
    this.transactionActive = false;
    this.optimisticLockError = false;
    this.resetPageEntriesFromBackendPages();

    return true;
  }

  async runTransaction<T>(transactionFn: () => Promise<T> | T, retries = 0): Promise<TransactionResult<T>> {
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
      for (let retry = 0; retry <= retries; ++retry) {
        if (retry > 0) {
          // we are in a retry, so refresh first
          this.refresh();
          await this.loadingFinished();
        }

        this.optimisticLockError = false;
        const resultValue: T = await transactionFn();

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
      this.optimisticLockError = false;

      if (!result.committed) {
        // refresh and wait until loading is finished
        this.refresh();
        await this.loadingFinished();
      }
    }

    return result;
  }
}
