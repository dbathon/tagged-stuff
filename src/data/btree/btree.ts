import { compareUint8Arrays } from "../page-entries/compareUint8Arrays";
import {
  readPageEntriesCount,
  readPageEntryByNumber,
  scanPageEntries,
  scanPageEntriesReverse,
} from "../page-entries/pageEntries";
import { PageProvider } from "./pageProvider";

/**
 * The functions in this file allow treating "pages" (Uint8Arrays) as nodes of a B+-tree.
 *
 * The implementation uses the pageEntries functionality to handle the individual pages.
 *
 * All pages are assumed to have the same size. Entries can at most have half the size of the pages and at most 2000
 * bytes (that is a restriction of pageEntries). The empty array is not allowed as an entry.
 *
 * The returned entries often share the underlying ArrayBuffer with the pages (to avoid unnecessary copies).
 * So the returned entries can change if the pages are modified, if necessary the caller should copy the returned
 * entries.
 */

function checkIsNotEmptyArray(entry: Uint8Array | undefined): void {
  if (entry?.length === 0) {
    throw new Error("empty array is not allowed");
  }
}

const CONTINUE = 1;
const ABORTED = 2;
const MISSING_PAGE = 3;
type ScanResult = typeof CONTINUE | typeof ABORTED | typeof MISSING_PAGE;

function scan(
  pageProvider: PageProvider,
  pageNumber: number,
  startEntry: Uint8Array | undefined,
  startEntryWithZeroPrefix: Uint8Array | undefined,
  forward: boolean,
  isRootPage: boolean,
  callback: (entry: Uint8Array) => boolean
): ScanResult {
  const pageArray = pageProvider(pageNumber);
  if (!pageArray) {
    return MISSING_PAGE;
  }
  const entryCount = readPageEntriesCount(pageArray);
  if (entryCount < 1) {
    if (isRootPage) {
      // empty btree
      return CONTINUE;
    } else {
      throw new Error("unexpected empty page: " + pageNumber);
    }
  }
  const isLeafPage = readPageEntryByNumber(pageArray, 0).length === 0;
  if (isLeafPage) {
    let aborted = false;
    const wrappedCallback = (entry: Uint8Array) => {
      if (entry.length === 0) {
        // this is the marker entry for the leaf page, just continue/skip
        return true;
      }
      const callbackResult = callback(entry);
      if (!callbackResult) {
        aborted = true;
      }
      return callbackResult;
    };
    if (forward) {
      scanPageEntries(pageArray, startEntry, wrappedCallback);
    } else {
      scanPageEntriesReverse(pageArray, startEntry, wrappedCallback);
    }
    return aborted ? ABORTED : CONTINUE;
  } else {
    if (entryCount < 3 || entryCount % 2 === 0) {
      throw new Error("invalid entryCount for non-leaf node: " + entryCount);
    }
    const minChildIndex = entryCount >> 1;
    let startChildIndex = forward ? minChildIndex : entryCount - 1;
    if (startEntry !== undefined) {
      if (startEntryWithZeroPrefix === undefined) {
        // lazy init
        startEntryWithZeroPrefix = new Uint8Array(startEntry.length + 1);
        startEntryWithZeroPrefix[0] = 0;
        startEntryWithZeroPrefix.set(startEntry, 1);
      }
      if (forward) {
        // scan backward to find the entry before or equal
        scanPageEntriesReverse(pageArray, startEntryWithZeroPrefix, (_, entryNumber) => {
          startChildIndex = minChildIndex + entryNumber + 1;
          return false;
        });
      } else {
        // scan forward to find the entry after or equal
        scanPageEntries(pageArray, startEntryWithZeroPrefix, (entry, entryNumber) => {
          const compareResult = compareUint8Arrays(entry, startEntryWithZeroPrefix!);
          // if entry matches startEntry exactly, then we also need to scan its right child
          // (it might contain start entry)
          startChildIndex = minChildIndex + entryNumber + (compareResult === 0 ? 1 : 0);
          return false;
        });
      }
      if (startChildIndex === -1) {
        return CONTINUE;
      }
    }

    const direction = forward ? 1 : -1;
    for (
      let childIndex = startChildIndex;
      childIndex >= minChildIndex && childIndex < entryCount;
      childIndex += direction
    ) {
      const childEntry = readPageEntryByNumber(pageArray, childIndex);
      if (childEntry.length !== 6) {
        throw new Error("unexpected child entry length: " + childEntry.length);
      }
      const childPageNumber = new DataView(childEntry.buffer, childEntry.byteOffset).getUint32(2);
      const childScanResult = scan(
        pageProvider,
        childPageNumber,
        startEntry,
        startEntryWithZeroPrefix,
        forward,
        false,
        callback
      );
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
  checkIsNotEmptyArray(startEntry);
  const result = scan(pageProvider, rootPageNumber, startEntry, undefined, true, true, callback);
  return result !== MISSING_PAGE;
}

export function scanBtreeEntriesReverse(
  pageProvider: PageProvider,
  rootPageNumber: number,
  startEntry: Uint8Array | undefined,
  callback: (entry: Uint8Array) => boolean
): boolean {
  checkIsNotEmptyArray(startEntry);
  const result = scan(pageProvider, rootPageNumber, startEntry, undefined, false, true, callback);
  return result !== MISSING_PAGE;
}
