import { compareUint8Arrays } from "../page-entries/compareUint8Arrays";
import {
  containsPageEntry,
  getIndexOfPageEntry,
  insertPageEntry,
  readPageEntriesCount,
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

const NODE_TYPE_LEAF = 1;
const NODE_TYPE_INNER = 2;
type NodeType = typeof NODE_TYPE_LEAF | typeof NODE_TYPE_INNER;

function getNodeType(pageArray: Uint8Array): NodeType {
  // version and node type are stored in the first byte
  const versionAndType = pageArray[0];
  const version = versionAndType >> 4;
  if (version !== VERSION) {
    throw new Error("unexpected version: " + version);
  }
  const type = versionAndType & 0xf;
  if (type !== NODE_TYPE_LEAF && type !== NODE_TYPE_INNER) {
    throw new Error("unexpected node type: " + type);
  }
  return type;
}

/**
 * The child page numbers are stored in a separate fixed size uint32 array before the page entries. For that array we
 * reserve 25% of the page size (because the actual entries are maybe 12 bytes with overhead, but that is just a
 * guess...).
 */
function getInnerNodeMaxChildPageNumbers(pageArray: Uint8Array): number {
  // divide by 16 and floor (a quarter of the pages size and 4 bytes per page number)
  const result = pageArray.length >> 4;
  if (result < 3) {
    // a page with less than 3 children cannot be split properly if a new child is added
    throw new Error("the page is too small: " + pageArray.length);
  }
  return result;
}

function getEntriesPageArray(pageArray: Uint8Array, nodeType: NodeType): Uint8Array {
  const offset = nodeType === NODE_TYPE_LEAF ? 1 : 1 + (getInnerNodeMaxChildPageNumbers(pageArray) << 2);
  return new Uint8Array(pageArray.buffer, pageArray.byteOffset + offset, pageArray.byteLength - offset);
}

function getInnerNodeChildPageNumbersDataView(pageArray: Uint8Array): DataView {
  const maxChildPageNumbers = getInnerNodeMaxChildPageNumbers(pageArray);
  return new DataView(pageArray.buffer, pageArray.byteOffset + 1, maxChildPageNumbers << 2);
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
  const nodeType = getNodeType(pageArray);
  const entriesPageArray = getEntriesPageArray(pageArray, nodeType);
  const entryCount = readPageEntriesCount(entriesPageArray);
  if (nodeType === NODE_TYPE_LEAF) {
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
    if (entryCount < 1) {
      throw new Error("no entries for inner node: " + entryCount);
    }
    let startChildPageNumberIndex = forward ? 0 : entryCount;
    if (startEntry !== undefined) {
      if (forward) {
        // scan backward to find the entry before or equal
        scanPageEntriesReverse(entriesPageArray, startEntry, (_, entryNumber) => {
          startChildPageNumberIndex = entryNumber + 1;
          return false;
        });
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
      if (startChildPageNumberIndex === -1) {
        return CONTINUE;
      }
    }

    const childPageNumbersDataView = getInnerNodeChildPageNumbersDataView(pageArray);
    if (childPageNumbersDataView.byteLength < (entryCount + 1) << 2) {
      // this should not happen (should be prevented by the insert logic)
      throw new Error("too many entries for inner node");
    }
    const direction = forward ? 1 : -1;
    for (
      let childIndex = startChildPageNumberIndex;
      childIndex >= 0 && childIndex <= entryCount;
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

function initPageArray(pageArray: Uint8Array, nodeType: NodeType): void {
  pageArray[0] = (VERSION << 4) | nodeType;
  resetPageEntries(getEntriesPageArray(pageArray, nodeType));
}

function allocateAndInitPage(pageProvider: PageProviderForWrite, nodeType: NodeType): number {
  const pageNumber = pageProvider.allocateNewPage();
  initPageArray(pageProvider.getPageForUpdate(pageNumber), nodeType);
  return pageNumber;
}

export function allocateAndInitBtreeRootPage(pageProvider: PageProviderForWrite): number {
  return allocateAndInitPage(pageProvider, NODE_TYPE_LEAF);
}

function insertPageEntryWithThrow(pageArray: Uint8Array, entry: Uint8Array): void {
  if (!insertPageEntry(pageArray, entry)) {
    throw new Error("entry insert failed unexpectedly");
  }
}

function findSplitIndex(entries: Uint8Array[]): number {
  const totalCount = entries.length;
  if (totalCount < 2) {
    throw new Error("cannot split a node with less than 2 entries");
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

/**
 * This implementation might not be the most efficient, but it is relatively clear and simple...
 */
function splitLeafPageAndInsert(
  pageProvider: PageProviderForWrite,
  leftEntriesPageArrayForUpdate: Uint8Array,
  newEntry: Uint8Array
): NewChildPageResult {
  const allEntries = [newEntry];
  scanPageEntries(leftEntriesPageArrayForUpdate, undefined, (entry) => {
    // copy the array to make sure it stays stable
    allEntries.push(Uint8Array.from(entry));
    return true;
  });
  // sort to move newEntry to the correct place
  allEntries.sort(compareUint8Arrays);

  const totalCount = allEntries.length;
  const rightStartIndex = findSplitIndex(allEntries);
  const rightPageNumber = allocateAndInitPage(pageProvider, NODE_TYPE_LEAF);
  const rightEntriesPageArrayForUpdate = getEntriesPageArray(
    pageProvider.getPageForUpdate(rightPageNumber),
    NODE_TYPE_LEAF
  );

  // insert entries into right page
  for (let i = rightStartIndex; i < totalCount; i++) {
    insertPageEntryWithThrow(rightEntriesPageArrayForUpdate, allEntries[i]);
  }
  // remove entries from left page in reverse order
  for (let i = totalCount - 1; i >= rightStartIndex; i--) {
    removePageEntry(leftEntriesPageArrayForUpdate, allEntries[i]);
  }
  const newEntryIndex = allEntries.indexOf(newEntry);
  if (newEntryIndex < rightStartIndex) {
    // insert newEntry into the left page
    const insertSuccess = insertPageEntry(leftEntriesPageArrayForUpdate, newEntry);
    if (!insertSuccess) {
      // should be rare, but might happen because of "fragmentation" => reset the page and insert all entries again
      resetPageEntries(leftEntriesPageArrayForUpdate);
      for (let i = 0; i < rightStartIndex; i++) {
        insertPageEntryWithThrow(leftEntriesPageArrayForUpdate, allEntries[i]);
      }
    }
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

function insert(
  pageProvider: PageProviderForWrite,
  pageNumber: number,
  entry: Uint8Array,
  isRootPage: boolean
): boolean | NewChildPageResult {
  const pageArray = pageProvider.getPage(pageNumber);
  const nodeType = getNodeType(pageArray);
  const entriesPageArray = getEntriesPageArray(pageArray, nodeType);

  if (nodeType === NODE_TYPE_LEAF) {
    if (containsPageEntry(entriesPageArray, entry)) {
      // nothing to do
      return false;
    }
    // ensure the size constraint
    const maxLength = Math.min(entriesPageArray.length / 4, 2000);
    if (entry.length > maxLength) {
      throw new Error("entry is too large: " + entry.length);
    }
    const pageArrayForUpdate = pageProvider.getPageForUpdate(pageNumber);
    const entriesPageArrayForUpdate = getEntriesPageArray(pageArrayForUpdate, nodeType);
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
    const splitResult = splitLeafPageAndInsert(pageProvider, entriesPageArrayForUpdate, entry);
    if (rootPageArrayBefore) {
      // we want to just keep the root page, so allocate a new page for the left child
      const leftChildPageNumber = allocateAndInitPage(pageProvider, NODE_TYPE_LEAF);
      const leftChildEntriesPageArrayForUpdate = getEntriesPageArray(
        pageProvider.getPageForUpdate(leftChildPageNumber),
        NODE_TYPE_LEAF
      );
      scanPageEntries(entriesPageArrayForUpdate, undefined, (entryToCopy) => {
        insertPageEntryWithThrow(leftChildEntriesPageArrayForUpdate, entryToCopy);
        return true;
      });
      // now reset the root page and convert it into an inner page
      pageArrayForUpdate.set(rootPageArrayBefore);
      initPageArray(pageArrayForUpdate, NODE_TYPE_INNER);
      insertPageEntryWithThrow(getEntriesPageArray(pageArrayForUpdate, NODE_TYPE_INNER), splitResult.lowerBound);
      const childPageNumbersDataView = getInnerNodeChildPageNumbersDataView(pageArrayForUpdate);
      childPageNumbersDataView.setUint32(0, leftChildPageNumber);
      childPageNumbersDataView.setUint32(4, splitResult.childPageNumber);
      return true;
    } else {
      return splitResult;
    }
  } else {
    let childIndex = 0;
    scanPageEntriesReverse(entriesPageArray, entry, (_, index) => {
      childIndex = index + 1;
      return false;
    });
    const childPageNumber = getInnerNodeChildPageNumbersDataView(pageArray).getUint32(childIndex << 2);
    const childInsertResult = insert(pageProvider, childPageNumber, entry, false);
    if (typeof childInsertResult === "boolean") {
      return childInsertResult;
    } else {
      // we have a new child and need to insert it into this page
      const pageArrayForUpdate = pageProvider.getPageForUpdate(pageNumber);
      const entriesPageArrayForUpdate = getEntriesPageArray(pageArrayForUpdate, nodeType);
      const countBefore = readPageEntriesCount(entriesPageArrayForUpdate);
      const moreChildrenPossible = countBefore + 1 < getInnerNodeMaxChildPageNumbers(pageArrayForUpdate);
      const insertSuccess =
        moreChildrenPossible && insertPageEntry(entriesPageArrayForUpdate, childInsertResult.lowerBound);
      if (insertSuccess) {
        // sanity checks
        if (countBefore + 1 !== readPageEntriesCount(entriesPageArrayForUpdate)) {
          throw new Error("lowerBound already existed");
        }
        const insertedAtIndex = getIndexOfPageEntry(entriesPageArrayForUpdate, childInsertResult.lowerBound);
        if (insertedAtIndex !== childIndex) {
          throw new Error("unexpected insertedAtIndex: " + insertedAtIndex + ", " + childIndex);
        }
        writeChildPageNumber(
          getInnerNodeChildPageNumbersDataView(pageArrayForUpdate),
          childInsertResult.childPageNumber,
          childIndex + 1,
          countBefore + 1
        );
        return true;
      } else {
        if (countBefore < 2) {
          // this should not happen, but potentially could because of page entry fragmentation?...
          // TODO: we could potentially rewrite the pages or even split anyway and have a page that just has a child pointer...
          throw new Error("cannot split page with less than two entries");
        }

        throw new Error("TODO: splitting of inner node not implemented yet");
      }
    }
  }
}

export function insertBtreeEntry(
  pageProvider: PageProviderForWrite,
  rootPageNumber: number,
  entry: Uint8Array
): boolean {
  const insertResult = insert(pageProvider, rootPageNumber, entry, true);
  if (typeof insertResult !== "boolean") {
    throw new Error("insert on root node returned unexpected result");
  }
  return insertResult;
}
