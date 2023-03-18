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
 * bytes (that is a restriction of pageEntries).
 *
 * The returned entries often share the underlying ArrayBuffer with the pages (to avoid unnecessary copies).
 * So the returned entries can change if the pages are modified, if necessary the caller should copy the returned
 * entries.
 */

const NODE_TYPE_LEAF = 1;
const NODE_TYPE_INNER = 2;
type NodeType = typeof NODE_TYPE_LEAF | typeof NODE_TYPE_INNER;

function getNodeType(pageArray: Uint8Array): NodeType {
  // version and node type are stored in the first byte
  const versionAndType = pageArray[0];
  const version = versionAndType >> 4;
  if (version !== 1) {
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
  if (result < 2) {
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
