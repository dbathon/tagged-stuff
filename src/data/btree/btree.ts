import { compareUint8Arrays } from "../page-entries/compareUint8Arrays";
import {
  containsPageEntry,
  getEntryNumberOfPageEntry,
  insertPageEntry,
  readAllPageEntries,
  readPageEntriesCount,
  readPageEntriesFreeSpace,
  readPageEntryByNumber,
  removePageEntry,
  resetPageEntries,
  scanPageEntries,
  scanPageEntriesReverse,
} from "../page-entries/pageEntries";
import { PageProvider, PageProviderForWrite } from "./pageProvider";

/**
 * The functions in this file allow treating "pages" (Uint8Arrays) as nodes of a B+-tree.
 *
 * The implementation uses the pageEntries functionality to handle the individual pages.
 *
 * All pages are assumed to have the same size. Entries can at most have 25% of the size of the pages and at most 2000
 * bytes (that is a restriction of pageEntries).
 *
 * The returned entries often share the underlying ArrayBuffer with the pages (to avoid unnecessary copies).
 * So the returned entries can change if the pages are modified, if necessary the caller should copy the returned
 * entries.
 */

const VERSION = 1;

const PAGE_TYPE_LEAF = 1;
const PAGE_TYPE_INNER = 2;
type PageType = typeof PAGE_TYPE_LEAF | typeof PAGE_TYPE_INNER;

function getPageType(pageArray: Uint8Array): PageType {
  // version and page type are stored in the first byte
  const versionAndType = pageArray[0];
  const version = versionAndType >> 4;
  if (version !== VERSION) {
    throw new Error("unexpected version: " + version);
  }
  const type = versionAndType & 0xf;
  if (type !== PAGE_TYPE_LEAF && type !== PAGE_TYPE_INNER) {
    throw new Error("unexpected page type: " + type);
  }
  return type;
}

/**
 * The child page numbers are stored in a separate fixed size uint32 array before the page entries. For that array we
 * reserve 25% of the page size (because the actual entries are maybe 12 bytes with overhead, but that is just a
 * guess...).
 */
function getInnerPageMaxChildPageNumbers(pageArray: Uint8Array): number {
  // divide by 16 and floor (a quarter of the pages size and 4 bytes per page number)
  const result = pageArray.length >> 4;
  if (result < 3) {
    // a page with less than 3 children cannot be split properly if a new child is added
    throw new Error("the page is too small: " + pageArray.length);
  }
  return result;
}

function getEntriesPageArray(pageArray: Uint8Array, pageType: PageType): Uint8Array {
  const offset = pageType === PAGE_TYPE_LEAF ? 1 : 1 + (getInnerPageMaxChildPageNumbers(pageArray) << 2);
  return new Uint8Array(pageArray.buffer, pageArray.byteOffset + offset, pageArray.byteLength - offset);
}

function getInnerPageChildPageNumbersDataView(pageArray: Uint8Array): DataView {
  const maxChildPageNumbers = getInnerPageMaxChildPageNumbers(pageArray);
  return new DataView(pageArray.buffer, pageArray.byteOffset + 1, maxChildPageNumbers << 2);
}

function findChildIndex(entriesPageArray: Uint8Array, entry: Uint8Array): number {
  let childIndex = 0;
  // scan backward to find the entry before or equal
  scanPageEntriesReverse(entriesPageArray, entry, (_, index) => {
    childIndex = index + 1;
    return false;
  });
  return childIndex;
}

const CONTINUE = 1;
const ABORTED = 2;
const MISSING_PAGE = 3;
type ScanResult = typeof CONTINUE | typeof ABORTED | typeof MISSING_PAGE;

function scan(
  pageProvider: PageProvider,
  pageNumber: number,
  startEntry: Uint8Array | undefined,
  forward: boolean,
  callback: (entry: Uint8Array) => boolean
): ScanResult {
  const pageArray = pageProvider(pageNumber);
  if (!pageArray) {
    return MISSING_PAGE;
  }
  const pageType = getPageType(pageArray);
  const entriesPageArray = getEntriesPageArray(pageArray, pageType);
  const entriesCount = readPageEntriesCount(entriesPageArray);
  if (pageType === PAGE_TYPE_LEAF) {
    let aborted = false;
    const wrappedCallback = (entry: Uint8Array) => {
      const callbackResult = callback(entry);
      if (!callbackResult) {
        aborted = true;
      }
      return callbackResult;
    };
    if (forward) {
      scanPageEntries(entriesPageArray, startEntry, wrappedCallback);
    } else {
      scanPageEntriesReverse(entriesPageArray, startEntry, wrappedCallback);
    }
    return aborted ? ABORTED : CONTINUE;
  } else {
    let startChildPageNumberIndex = forward ? 0 : entriesCount;
    if (startEntry !== undefined) {
      if (forward) {
        startChildPageNumberIndex = findChildIndex(entriesPageArray, startEntry);
      } else {
        // scan forward to find the entry after or equal
        scanPageEntries(entriesPageArray, startEntry, (entry, entryNumber) => {
          const compareResult = compareUint8Arrays(entry, startEntry);
          // if entry matches startEntry exactly, then we also need to scan its right child
          // (it might contain start entry)
          startChildPageNumberIndex = entryNumber + (compareResult === 0 ? 1 : 0);
          return false;
        });
      }
    }

    const childPageNumbersDataView = getInnerPageChildPageNumbersDataView(pageArray);
    if (childPageNumbersDataView.byteLength < (entriesCount + 1) << 2) {
      // this should not happen (should be prevented by the insert logic)
      throw new Error("too many entries for inner page");
    }
    const direction = forward ? 1 : -1;
    for (
      let childIndex = startChildPageNumberIndex;
      childIndex >= 0 && childIndex <= entriesCount;
      childIndex += direction
    ) {
      const childPageNumber = childPageNumbersDataView.getUint32(childIndex << 2);
      const childScanResult = scan(pageProvider, childPageNumber, startEntry, forward, callback);
      if (childScanResult !== CONTINUE) {
        return childScanResult;
      }
    }
    return CONTINUE;
  }
}

export function scanBtreeEntries(
  pageProvider: PageProvider,
  rootPageNumber: number,
  startEntry: Uint8Array | undefined,
  callback: (entry: Uint8Array) => boolean
): boolean {
  const result = scan(pageProvider, rootPageNumber, startEntry, true, callback);
  return result !== MISSING_PAGE;
}

export function scanBtreeEntriesReverse(
  pageProvider: PageProvider,
  rootPageNumber: number,
  startEntry: Uint8Array | undefined,
  callback: (entry: Uint8Array) => boolean
): boolean {
  const result = scan(pageProvider, rootPageNumber, startEntry, false, callback);
  return result !== MISSING_PAGE;
}

export function checkIntegrity(
  pageProvider: PageProvider,
  pageNumber: number
): { depth: number; firstEntry?: Uint8Array; lastEntry?: Uint8Array } {
  const pageArray = pageProvider(pageNumber);
  if (!pageArray) {
    throw new Error("page is missing: " + pageNumber);
  }
  const pageType = getPageType(pageArray);
  const entriesPageArray = getEntriesPageArray(pageArray, pageType);
  const entriesCount = readPageEntriesCount(entriesPageArray);
  if (pageType === PAGE_TYPE_LEAF) {
    if (entriesCount === 0) {
      // probably an empty root page
      return {
        depth: 1,
      };
    } else {
      // we assume, that the order in the page entries is correct...
      return {
        depth: 1,
        firstEntry: readPageEntryByNumber(entriesPageArray, 0),
        lastEntry: readPageEntryByNumber(entriesPageArray, entriesCount - 1),
      };
    }
  } else {
    let childDepth: number | undefined = undefined;
    let firstEntry: Uint8Array | undefined = undefined;
    let lastEntry: Uint8Array | undefined = undefined;
    const childPageNumbersDataView = getInnerPageChildPageNumbersDataView(pageArray);
    const pageEntries = readAllPageEntries(entriesPageArray);
    for (let i = 0; i <= entriesCount; i++) {
      const childResult = checkIntegrity(pageProvider, childPageNumbersDataView.getUint32(i << 2));
      if (childDepth === undefined) {
        childDepth = childResult.depth;
      } else if (childDepth !== childResult.depth) {
        throw new Error("different depth in child trees: " + childDepth + ", " + childResult.depth);
      }
      if (!firstEntry) {
        firstEntry = childResult.firstEntry;
      }
      if (childResult.lastEntry) {
        lastEntry = childResult.lastEntry;
      }
      if (i < entriesCount && childResult.lastEntry) {
        const upperBoundExclusive = pageEntries[i];
        if (compareUint8Arrays(childResult.lastEntry, upperBoundExclusive) >= 0) {
          throw new Error("last entry of child tree too large: " + pageNumber + ", " + i);
        }
      }
      if (i > 0 && childResult.firstEntry) {
        const lowerBound = pageEntries[i - 1];
        if (compareUint8Arrays(lowerBound, childResult.firstEntry) > 0) {
          throw new Error("first entry of child tree too large: " + pageNumber + ", " + i);
        }
      }
    }
    if (childDepth === undefined) {
      // this should never happen, since entriesCount is at least 0
      throw new Error("unexpected");
    }
    if (!firstEntry || !lastEntry) {
      throw new Error("no entries in tree of page " + pageNumber);
    }
    return {
      depth: childDepth + 1,
      firstEntry,
      lastEntry,
    };
  }
}

/**
 * Scans the whole tree and checks that all invariants are met, in particular equal depth for all child trees and
 * proper order of the entries. If there is any problem, then an error is thrown.
 *
 * This method expects that the pageProvider has all pages available.
 */
export function checkBtreeIntegrity(pageProvider: PageProvider, rootPageNumber: number): void {
  checkIntegrity(pageProvider, rootPageNumber);
}

function initPageArray(pageArray: Uint8Array, pageType: PageType): void {
  pageArray[0] = (VERSION << 4) | pageType;
  resetPageEntries(getEntriesPageArray(pageArray, pageType));
}

function allocateAndInitPage(pageProvider: PageProviderForWrite, pageType: PageType): number {
  const pageNumber = pageProvider.allocateNewPage();
  initPageArray(pageProvider.getPageForUpdate(pageNumber), pageType);
  return pageNumber;
}

export function allocateAndInitBtreeRootPage(pageProvider: PageProviderForWrite): number {
  return allocateAndInitPage(pageProvider, PAGE_TYPE_LEAF);
}

function insertPageEntryWithThrow(pageArray: Uint8Array, entry: Uint8Array, tryRewrite = false): void {
  if (!insertPageEntry(pageArray, entry)) {
    if (!tryRewrite) {
      throw new Error("entry insert failed unexpectedly");
    }
    // reset the page and insert all entries again (including the new entry) to remove potential "fragmentation"
    const oldEntries = readAllPageEntries(pageArray).map((entry) => Uint8Array.from(entry));
    resetPageEntries(pageArray);
    for (const oldEntry of oldEntries) {
      insertPageEntryWithThrow(pageArray, oldEntry);
    }
    insertPageEntryWithThrow(pageArray, entry);
  }
}

function findSplitIndex(entries: Uint8Array[]): number {
  const totalCount = entries.length;
  if (totalCount < 2) {
    throw new Error("cannot split a page with less than 2 entries");
  }
  const halfTotalBytes = entries.reduce((sum, entry) => sum + entry.length, 0) >> 1;
  let rightStartIndex = 1;
  let leftBytes = entries[0].length;
  while (leftBytes < halfTotalBytes && rightStartIndex < totalCount - 1) {
    leftBytes += entries[rightStartIndex].length;
    rightStartIndex++;
  }
  return rightStartIndex;
}

interface NewChildPageResult {
  lowerBound: Uint8Array;
  childPageNumber: number;
}

function getSortedEntriesWithNewEntry(entriesPageArray: Uint8Array, newEntry: Uint8Array): Uint8Array[] {
  const entries = [newEntry];
  scanPageEntries(entriesPageArray, undefined, (entry) => {
    // copy the array to make sure it stays stable
    entries.push(Uint8Array.from(entry));
    return true;
  });
  // sort to move newEntry to the correct place
  entries.sort(compareUint8Arrays);
  return entries;
}

/**
 * This implementation might not be the most efficient, but it is relatively clear and simple...
 */
function splitLeafPageAndInsert(
  pageProvider: PageProviderForWrite,
  leftEntriesPageArrayForUpdate: Uint8Array,
  newEntry: Uint8Array,
  isRightMostSibling: boolean
): NewChildPageResult {
  const allEntries = getSortedEntriesWithNewEntry(leftEntriesPageArrayForUpdate, newEntry);
  const newEntryIndex = allEntries.indexOf(newEntry);
  const totalCount = allEntries.length;

  // Optimization for mostly ascending inserts (this avoids leaving most pages half empty for those cases)
  const onlyNewEntryInNewPage = isRightMostSibling && newEntryIndex === totalCount - 1;

  const rightStartIndex = onlyNewEntryInNewPage ? newEntryIndex : findSplitIndex(allEntries);
  const rightPageNumber = allocateAndInitPage(pageProvider, PAGE_TYPE_LEAF);
  const rightEntriesPageArrayForUpdate = getEntriesPageArray(
    pageProvider.getPageForUpdate(rightPageNumber),
    PAGE_TYPE_LEAF
  );

  // insert entries into right page
  for (let i = rightStartIndex; i < totalCount; i++) {
    insertPageEntryWithThrow(rightEntriesPageArrayForUpdate, allEntries[i]);
  }
  // remove entries from left page in reverse order
  for (let i = totalCount - 1; i >= rightStartIndex; i--) {
    removePageEntry(leftEntriesPageArrayForUpdate, allEntries[i]);
  }
  if (newEntryIndex < rightStartIndex) {
    // insert newEntry into the left page
    insertPageEntryWithThrow(leftEntriesPageArrayForUpdate, newEntry, true);
  }

  // sanity check
  const leftCount = readPageEntriesCount(leftEntriesPageArrayForUpdate);
  const rightCount = readPageEntriesCount(rightEntriesPageArrayForUpdate);
  if (leftCount < 1 || rightCount < 1 || leftCount + rightCount !== totalCount) {
    throw new Error("unexpected counts: " + leftCount + ", " + rightCount + ", " + totalCount);
  }

  // determine the lowerBound
  const left = allEntries[rightStartIndex - 1];
  const right = allEntries[rightStartIndex];
  for (let i = 0; ; i++) {
    const leftByte = left[i];
    const rightByte = right[i];
    if (leftByte !== rightByte) {
      if (rightByte === undefined) {
        throw new Error("unexpected undefined byte");
      }
      return {
        lowerBound: right.slice(0, i + 1),
        childPageNumber: rightPageNumber,
      };
    }
  }
}

/**
 * This implementation might not be the most efficient, but it is relatively clear and simple...
 */
function splitInnerPageAndInsert(
  pageProvider: PageProviderForWrite,
  leftPageArrayForUpdate: Uint8Array,
  leftEntriesPageArrayForUpdate: Uint8Array,
  childInsertResult: NewChildPageResult,
  isRightMostSibling: boolean
): NewChildPageResult {
  const allEntries = getSortedEntriesWithNewEntry(leftEntriesPageArrayForUpdate, childInsertResult.lowerBound);

  // totalCount is also the old child count
  const totalCount = allEntries.length;
  if (totalCount < 3) {
    throw new Error("cannot split inner page with less then 3 entries");
  }
  const allChildPageNumbers: number[] = [];
  const leftChildPageNumbersDataView = getInnerPageChildPageNumbersDataView(leftPageArrayForUpdate);
  for (let i = 0; i < totalCount; i++) {
    allChildPageNumbers.push(leftChildPageNumbersDataView.getUint32(i << 2));
  }
  const newEntryIndex = allEntries.indexOf(childInsertResult.lowerBound);
  // insert the new child number
  allChildPageNumbers.splice(newEntryIndex + 1, 0, childInsertResult.childPageNumber);

  // Optimization for mostly ascending inserts (this avoids leaving most pages half empty for those cases)
  const onlyNewEntryInNewPage = isRightMostSibling && newEntryIndex === totalCount - 1;

  const splitIndex = onlyNewEntryInNewPage ? newEntryIndex : findSplitIndex(allEntries);
  const middleIndex = splitIndex > 1 ? splitIndex - 1 : splitIndex;

  const rightStartIndex = middleIndex + 1;
  const rightPageNumber = allocateAndInitPage(pageProvider, PAGE_TYPE_INNER);
  const rightPageArrayForUpdate = pageProvider.getPageForUpdate(rightPageNumber);
  const rightEntriesPageArrayForUpdate = getEntriesPageArray(rightPageArrayForUpdate, PAGE_TYPE_INNER);

  // insert entries into right page
  for (let i = rightStartIndex; i < totalCount; i++) {
    insertPageEntryWithThrow(rightEntriesPageArrayForUpdate, allEntries[i]);
  }
  // write child page numbers in right page
  const rightChildPageNumbersDataView = getInnerPageChildPageNumbersDataView(rightPageArrayForUpdate);
  for (let i = rightStartIndex; i <= totalCount; i++) {
    rightChildPageNumbersDataView.setUint32((i - rightStartIndex) << 2, allChildPageNumbers[i]);
  }

  // remove entries from left page in reverse order
  for (let i = totalCount - 1; i >= middleIndex; i--) {
    removePageEntry(leftEntriesPageArrayForUpdate, allEntries[i]);
  }
  if (newEntryIndex < middleIndex) {
    // insert lowerBound into the left page
    insertPageEntryWithThrow(leftEntriesPageArrayForUpdate, childInsertResult.lowerBound, true);
    // in this case we also need to rewrite some child page numbers
    for (let i = newEntryIndex + 1; i <= middleIndex; i++) {
      leftChildPageNumbersDataView.setUint32(i << 2, allChildPageNumbers[i]);
    }
  }

  // sanity check
  const leftCount = readPageEntriesCount(leftEntriesPageArrayForUpdate);
  const rightCount = readPageEntriesCount(rightEntriesPageArrayForUpdate);
  if (leftCount < 1 || rightCount < 1 || leftCount + rightCount !== totalCount - 1) {
    throw new Error("unexpected counts: " + leftCount + ", " + rightCount + ", " + totalCount);
  }

  return {
    lowerBound: allEntries[middleIndex],
    childPageNumber: rightPageNumber,
  };
}

function writeChildPageNumber(
  childPageNumbersDataView: DataView,
  childPageNumber: number,
  index: number,
  countBefore: number
): void {
  // move entries after the new entry to the right
  for (let i = countBefore; i > index; i--) {
    childPageNumbersDataView.setUint32(i << 2, childPageNumbersDataView.getUint32((i - 1) << 2));
  }
  // write the childPageNumber
  childPageNumbersDataView.setUint32(index << 2, childPageNumber);
}

function removeChildPageNumber(childPageNumbersDataView: DataView, index: number, countBefore: number): void {
  // move entries after the the removed entry to the left
  for (let i = index + 1; i < countBefore; i++) {
    childPageNumbersDataView.setUint32((i - 1) << 2, childPageNumbersDataView.getUint32(i << 2));
  }
}

function copyPageContent(fromPageArray: Uint8Array, toPageArrayForUpdate: Uint8Array, pageType: PageType) {
  const toEntriesPageArrayForUpdate = getEntriesPageArray(toPageArrayForUpdate, pageType);
  // copy the entries
  scanPageEntries(getEntriesPageArray(fromPageArray, pageType), undefined, (entryToCopy) => {
    insertPageEntryWithThrow(toEntriesPageArrayForUpdate, entryToCopy);
    return true;
  });
  // copy the child page numbers if necessary
  if (pageType === PAGE_TYPE_INNER) {
    const childPageNumbersCount = readPageEntriesCount(toEntriesPageArrayForUpdate) + 1;
    const bytesToCopy = childPageNumbersCount << 2;
    const childPageNumbersDataView = getInnerPageChildPageNumbersDataView(fromPageArray);
    if (bytesToCopy > childPageNumbersDataView.byteLength) {
      throw new Error("unexpected number of child page numbers");
    }
    // create a Uint8Array view to be able to use set
    toPageArrayForUpdate.set(
      new Uint8Array(childPageNumbersDataView.buffer, childPageNumbersDataView.byteOffset, bytesToCopy),
      1
    );
  }
}

function finishRootPageSplit(
  pageProvider: PageProviderForWrite,
  rootPageArrayForUpdate: Uint8Array,
  rootPageArrayBefore: Uint8Array,
  splitResult: NewChildPageResult
): void {
  // we want to just keep the root page, so allocate a new page for the left child
  const pageType = getPageType(rootPageArrayForUpdate);
  const leftChildPageNumber = allocateAndInitPage(pageProvider, pageType);
  const leftChildPageArrayForUpdate = pageProvider.getPageForUpdate(leftChildPageNumber);

  copyPageContent(rootPageArrayForUpdate, leftChildPageArrayForUpdate, pageType);

  // now reset the root page and convert it into an inner page with two children
  rootPageArrayForUpdate.set(rootPageArrayBefore);
  initPageArray(rootPageArrayForUpdate, PAGE_TYPE_INNER);
  insertPageEntryWithThrow(getEntriesPageArray(rootPageArrayForUpdate, PAGE_TYPE_INNER), splitResult.lowerBound);
  const childPageNumbersDataView = getInnerPageChildPageNumbersDataView(rootPageArrayForUpdate);
  childPageNumbersDataView.setUint32(0, leftChildPageNumber);
  childPageNumbersDataView.setUint32(4, splitResult.childPageNumber);
}

function insert(
  pageProvider: PageProviderForWrite,
  pageNumber: number,
  entry: Uint8Array,
  isRootPage: boolean,
  isRightMostSibling: boolean
): boolean | NewChildPageResult {
  const pageArray = pageProvider.getPage(pageNumber);
  const pageType = getPageType(pageArray);
  const entriesPageArray = getEntriesPageArray(pageArray, pageType);

  if (pageType === PAGE_TYPE_LEAF) {
    if (containsPageEntry(entriesPageArray, entry)) {
      // nothing to do
      return false;
    }
    // ensure the size constraint
    const maxLength = Math.min(pageArray.length / 4, 2000);
    if (entry.length > maxLength) {
      throw new Error("entry is too large: " + entry.length);
    }
    const pageArrayForUpdate = pageProvider.getPageForUpdate(pageNumber);
    const entriesPageArrayForUpdate = getEntriesPageArray(pageArrayForUpdate, pageType);
    const insertSuccess = insertPageEntry(entriesPageArrayForUpdate, entry);
    if (insertSuccess) {
      return true;
    }

    // we need to split the page
    let rootPageArrayBefore: Uint8Array | undefined = undefined;
    if (isRootPage) {
      // copy the page, before the split to minimize the diff
      rootPageArrayBefore = Uint8Array.from(pageArrayForUpdate);
    }
    const splitResult = splitLeafPageAndInsert(pageProvider, entriesPageArrayForUpdate, entry, isRightMostSibling);
    if (rootPageArrayBefore) {
      finishRootPageSplit(pageProvider, pageArrayForUpdate, rootPageArrayBefore, splitResult);
      return true;
    } else {
      return splitResult;
    }
  } else {
    const childIndex = findChildIndex(entriesPageArray, entry);
    const childPageNumber = getInnerPageChildPageNumbersDataView(pageArray).getUint32(childIndex << 2);
    const entriesCount = readPageEntriesCount(entriesPageArray);
    const childInsertResult = insert(pageProvider, childPageNumber, entry, false, childIndex === entriesCount);
    if (typeof childInsertResult === "boolean") {
      return childInsertResult;
    } else {
      // we have a new child and need to insert it into this page
      const pageArrayForUpdate = pageProvider.getPageForUpdate(pageNumber);
      const entriesPageArrayForUpdate = getEntriesPageArray(pageArrayForUpdate, pageType);
      const countBefore = readPageEntriesCount(entriesPageArrayForUpdate);
      const moreChildrenPossible = countBefore + 1 < getInnerPageMaxChildPageNumbers(pageArrayForUpdate);
      const insertSuccess =
        moreChildrenPossible && insertPageEntry(entriesPageArrayForUpdate, childInsertResult.lowerBound);
      if (insertSuccess) {
        // sanity checks
        if (countBefore + 1 !== readPageEntriesCount(entriesPageArrayForUpdate)) {
          throw new Error("lowerBound already existed");
        }
        const insertedAtIndex = getEntryNumberOfPageEntry(entriesPageArrayForUpdate, childInsertResult.lowerBound);
        if (insertedAtIndex !== childIndex) {
          throw new Error("unexpected insertedAtIndex: " + insertedAtIndex + ", " + childIndex);
        }
        writeChildPageNumber(
          getInnerPageChildPageNumbersDataView(pageArrayForUpdate),
          childInsertResult.childPageNumber,
          childIndex + 1,
          countBefore + 1
        );
        return true;
      } else {
        // we need to split the page
        if (countBefore < 2) {
          // this should not happen, but potentially could because of page entry fragmentation?...
          // TODO: we could potentially rewrite the pages or even split anyway and have a page that just has a child pointer...
          throw new Error("cannot split page with less than two entries");
        }

        let rootPageArrayBefore: Uint8Array | undefined = undefined;
        if (isRootPage) {
          // copy the page, before the split to minimize the diff
          rootPageArrayBefore = Uint8Array.from(pageArrayForUpdate);
        }
        const splitResult = splitInnerPageAndInsert(
          pageProvider,
          pageArrayForUpdate,
          entriesPageArrayForUpdate,
          childInsertResult,
          isRightMostSibling
        );
        if (rootPageArrayBefore) {
          finishRootPageSplit(pageProvider, pageArrayForUpdate, rootPageArrayBefore, splitResult);
          return true;
        } else {
          return splitResult;
        }
      }
    }
  }
}

export function insertBtreeEntry(
  pageProvider: PageProviderForWrite,
  rootPageNumber: number,
  entry: Uint8Array
): boolean {
  const insertResult = insert(pageProvider, rootPageNumber, entry, true, true);
  if (typeof insertResult !== "boolean") {
    throw new Error("insert on root page returned unexpected result");
  }
  return insertResult;
}

// attempt to merge pages if both of them have 70% of free space
const MERGE_THRESHOLD = 0.7;

function canBeMerged(
  pageArray: Uint8Array,
  entriesPageArray: Uint8Array,
  pageType: PageType,
  extraEntry?: Uint8Array
): boolean {
  const freeSpace = readPageEntriesFreeSpace(entriesPageArray) - (extraEntry?.length ?? 0);
  const freeSpacePercent = freeSpace / entriesPageArray.length;
  if (freeSpacePercent < MERGE_THRESHOLD) {
    return false;
  }
  if (pageType === PAGE_TYPE_INNER) {
    const childCount = readPageEntriesCount(entriesPageArray) + 1;
    const maxChildCount = getInnerPageMaxChildPageNumbers(pageArray);
    const freeChildCountPercent = (maxChildCount - childCount) / maxChildCount;
    if (freeChildCountPercent < MERGE_THRESHOLD) {
      return false;
    }
  }

  return true;
}

function maybeMergeIntoLeftSibling(
  pageProvider: PageProviderForWrite,
  pageArray: Uint8Array,
  entriesPageArray: Uint8Array,
  entryToRemove: Uint8Array,
  childIndexToRemove: number | undefined,
  parentPageArray: Uint8Array | undefined,
  parentChildIndex: number | undefined,
  pageType: PageType
): boolean {
  if (
    !parentPageArray ||
    parentChildIndex === undefined ||
    parentChildIndex < 1 ||
    !canBeMerged(pageArray, entriesPageArray, pageType)
  ) {
    return false;
  }

  const leftSiblingPageNumber = getInnerPageChildPageNumbersDataView(parentPageArray).getUint32(
    (parentChildIndex - 1) << 2
  );
  const leftSiblingPageArray = pageProvider.getPage(leftSiblingPageNumber);
  if (getPageType(leftSiblingPageArray) !== pageType) {
    throw new Error("left sibling page type does not match " + pageType);
  }

  let parentLowerBoundEntry: Uint8Array | undefined = undefined;
  if (pageType === PAGE_TYPE_INNER) {
    parentLowerBoundEntry = readPageEntryByNumber(
      getEntriesPageArray(parentPageArray, PAGE_TYPE_INNER),
      parentChildIndex - 1
    );
  }
  if (
    !canBeMerged(
      leftSiblingPageArray,
      getEntriesPageArray(leftSiblingPageArray, pageType),
      pageType,
      parentLowerBoundEntry
    )
  ) {
    return false;
  }
  let entryNumberToRemove = getEntryNumberOfPageEntry(entriesPageArray, entryToRemove);
  if (entryNumberToRemove === undefined) {
    throw new Error("sourceEntriesPageArray does not contain entryToRemove");
  }

  const leftSiblingPageArrayForUpdate = pageProvider.getPageForUpdate(leftSiblingPageNumber);
  const leftSiblingEntriesPageArrayForUpdate = getEntriesPageArray(leftSiblingPageArrayForUpdate, pageType);
  const leftSiblingEntryCountBefore = readPageEntriesCount(leftSiblingEntriesPageArrayForUpdate);
  if (parentLowerBoundEntry) {
    insertPageEntryWithThrow(leftSiblingEntriesPageArrayForUpdate, parentLowerBoundEntry, true);
  }
  scanPageEntries(entriesPageArray, undefined, (entry, entryNumber) => {
    if (entryNumber !== entryNumberToRemove) {
      // use tryRewrite = true, because the entries of the two pages should definitely fit into the left sibling page
      insertPageEntryWithThrow(leftSiblingEntriesPageArrayForUpdate, entry, true);
    }
    return true;
  });

  if (pageType === PAGE_TYPE_INNER) {
    if (childIndexToRemove === undefined) {
      throw new Error("childIndexToRemove is required for inner pages");
    }
    // handle the children
    const childPageNumbersDataView = getInnerPageChildPageNumbersDataView(pageArray);
    const childCount = readPageEntriesCount(entriesPageArray) + 1;
    const leftSiblingChildPageNumbersDataView = getInnerPageChildPageNumbersDataView(leftSiblingPageArrayForUpdate);
    let leftSiblingChildCount = leftSiblingEntryCountBefore + 1;
    for (let i = 0; i < childCount; i++) {
      if (i !== childIndexToRemove) {
        writeChildPageNumber(
          leftSiblingChildPageNumbersDataView,
          childPageNumbersDataView.getUint32(i << 2),
          leftSiblingChildCount,
          leftSiblingChildCount
        );
        leftSiblingChildCount++;
      }
    }
  }

  return true;
}

const REMOVE_CHILD_PAGE = 1;

/**
 * Implementation note: this implementation intentionally only implements merging a page into its left sibling page.
 * This simplifies the implementation significantly (less special cases) and should not lead to real problems. It can
 * lead to relatively empty leftmost leaf pages (not empty ones, those would be removed) and to leftmost inner pages
 * that have no entries and only one child page number, but that should be okay...
 */
function remove(
  pageProvider: PageProviderForWrite,
  pageNumber: number,
  entry: Uint8Array,
  parentPageArray: Uint8Array | undefined,
  parentChildIndex: number | undefined
): boolean | typeof REMOVE_CHILD_PAGE {
  const isRootPage = !parentPageArray;
  const pageArray = pageProvider.getPage(pageNumber);
  const pageType = getPageType(pageArray);
  const entriesPageArray = getEntriesPageArray(pageArray, pageType);

  if (pageType === PAGE_TYPE_LEAF) {
    if (!containsPageEntry(entriesPageArray, entry)) {
      // nothing to do
      return false;
    }

    if (!isRootPage) {
      const entriesCount = readPageEntriesCount(entriesPageArray);
      let removePage;
      if (entriesCount === 1) {
        // it is the only entry, just remove this page
        removePage = true;
      } else {
        removePage = maybeMergeIntoLeftSibling(
          pageProvider,
          pageArray,
          entriesPageArray,
          entry,
          undefined,
          parentPageArray,
          parentChildIndex,
          PAGE_TYPE_LEAF
        );
      }

      if (removePage) {
        pageProvider.releasePage(pageNumber);
        return REMOVE_CHILD_PAGE;
      }
    }

    const entriesPageArrayForUpdate = getEntriesPageArray(pageProvider.getPageForUpdate(pageNumber), pageType);
    return removePageEntry(entriesPageArrayForUpdate, entry);
  } else {
    const childIndex = findChildIndex(entriesPageArray, entry);
    const entriesCount = readPageEntriesCount(entriesPageArray);
    const childPageNumbersDataView = getInnerPageChildPageNumbersDataView(pageArray);
    const childPageNumber = childPageNumbersDataView.getUint32(childIndex << 2);
    const childRemoveResult = remove(pageProvider, childPageNumber, entry, pageArray, childIndex);
    if (typeof childRemoveResult === "boolean") {
      return childRemoveResult;
    } else {
      const entryNumberToRemove = Math.max(0, childIndex - 1);
      // the child needs to be removed
      if (isRootPage) {
        if (entriesCount === 0) {
          // should not happen, but we can still support it, the tree is now completely empty
          initPageArray(pageProvider.getPageForUpdate(pageNumber), PAGE_TYPE_LEAF);
          return true;
        } else if (entriesCount === 1) {
          const pageArrayForUpdate = pageProvider.getPageForUpdate(pageNumber);
          const remainingChildPageNumber = childPageNumbersDataView.getUint32((childIndex === 0 ? 1 : 0) << 2);
          const remainingChildPageArray = pageProvider.getPage(remainingChildPageNumber);
          const remainingChildPageType = getPageType(remainingChildPageArray);
          initPageArray(pageArrayForUpdate, remainingChildPageType);
          copyPageContent(remainingChildPageArray, pageArrayForUpdate, remainingChildPageType);
          pageProvider.releasePage(remainingChildPageNumber);
          return true;
        }
      } else {
        let removePage = false;
        if (entriesCount === 0) {
          // the last child was removed, so this node also needs to be removed
          removePage = true;
        } else if (
          /* do redundant checks here to avoid calling readPageEntryByNumber() if possible */
          parentChildIndex != undefined &&
          parentChildIndex > 0 &&
          canBeMerged(pageArray, entriesPageArray, PAGE_TYPE_INNER)
        ) {
          removePage = maybeMergeIntoLeftSibling(
            pageProvider,
            pageArray,
            entriesPageArray,
            readPageEntryByNumber(entriesPageArray, entryNumberToRemove),
            childIndex,
            parentPageArray,
            parentChildIndex,
            PAGE_TYPE_INNER
          );
        }

        if (removePage) {
          pageProvider.releasePage(pageNumber);
          return REMOVE_CHILD_PAGE;
        }
      }
      const pageArrayForUpdate = pageProvider.getPageForUpdate(pageNumber);
      const entriesPageArrayForUpdate = getEntriesPageArray(pageArrayForUpdate, pageType);
      const entryToRemove = readPageEntryByNumber(entriesPageArrayForUpdate, entryNumberToRemove);
      removePageEntry(entriesPageArrayForUpdate, entryToRemove);
      removeChildPageNumber(getInnerPageChildPageNumbersDataView(pageArrayForUpdate), childIndex, entriesCount + 1);
      return true;
    }
  }
}

export function removeBtreeEntry(
  pageProvider: PageProviderForWrite,
  rootPageNumber: number,
  entry: Uint8Array
): boolean {
  const removeResult = remove(pageProvider, rootPageNumber, entry, undefined, undefined);
  if (typeof removeResult !== "boolean") {
    throw new Error("remove on root page returned unexpected result");
  }
  return removeResult;
}
