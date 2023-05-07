import { assert } from "../misc/assert";
import { compareUint8Arrays } from "../uint8-array/compareUint8Arrays";

/**
 * The functions in this file allow treating "page data" (Uint8Array) like a sorted set of byte arrays (of up to 2000 bytes)...
 * The slightly complicated layout of the entries is to allow small diffs (bytes change "as little as possible").
 * The returned entries often share the underlying ArrayBuffer with the passed pageArray (to avoid unnecessary copies).
 * So the returned entries can change if pageArray is modified, if necessary the caller should copy the returned
 * entries.
 *
 * Page layout:
 * If the first byte is 0, then the page is just considered empty. Otherwise it is the "layout version" (currently always 1).
 * End of free space pointer (2 bytes): a pointer to the end (exclusive) of the free space between the entries array and the data
 * Free chunks pointer (2 bytes): a pointer to the first chunk of free space (may be 0)
 * Free chunks size (2 bytes): the sum of bytes available via the free chunks pointer
 * Entry count (2 bytes).
 * Entry pointers sorted by entry (2 bytes each)
 * Gap pointers (2 bytes each): pointers to the start of a gap between entries, the first two bytes of the gap denote its length (including those two bytes)
 *
 * Entry pointers point to a header of the entry. The header is two bytes (16 bit):
 *   bit f to b: reserved
 *   bit a to 0: length
 *
 * TODO: the implementation is not optimized for now...
 */

/**
 * The minimum page size is kind of arbitrary, but smaller pages don't really make sense.
 */
const MIN_PAGE_SIZE = 100;

/**
 * pageArray.length needs to fit into uint16 (see initIfNecessary()), so that is the max page size.
 */
const MAX_PAGE_SIZE = 0xffff;

/**
 * The maximum length of an entry. This is an arbitrary restrictions to avoid entries that take too much space of a
 * page..., but also helps with not requiring too many bits for the length in the chunk headers.
 */
const MAX_ENTRY_LENGTH = 2000;

// "pointers"
const FREE_SPACE_END_POINTER = 1;
const FREE_CHUNKS_SIZE = 3;
const ENTRY_COUNT = 5;
const ENTRIES = 7;

function getEntryPointerIndex(entryNumber: number): number {
  return ENTRIES + entryNumber * 2;
}

function readUint16(array: Uint8Array, index: number): number {
  return (array[index] << 8) | array[index + 1];
}

function writeUint16(array: Uint8Array, index: number, value: number): void {
  array[index] = (value >> 8) & 0xff;
  array[index + 1] = value & 0xff;
}

function checkPageArraySize(pageArray: Uint8Array) {
  if (pageArray.length < MIN_PAGE_SIZE) {
    throw new Error("page is too small");
  }
  if (pageArray.length > MAX_PAGE_SIZE) {
    throw new Error("page is too large");
  }
}

/** Also does some validation. */
export function readPageEntriesCount(pageArray: Uint8Array): number {
  checkPageArraySize(pageArray);
  const version = pageArray[0];
  if (version === 0) {
    // the page is empty
    return 0;
  }
  if (version !== 1) {
    throw new Error("unexpected version: " + version);
  }
  return readUint16(pageArray, ENTRY_COUNT);
}

export function resetPageEntries(pageArray: Uint8Array): void {
  checkPageArraySize(pageArray);
  // just set the first byte to 0 (see readPageEntriesCount()
  pageArray[0] = 0;
}

export function readPageEntriesFreeSpace(pageArray: Uint8Array): number {
  const entryCount = readPageEntriesCount(pageArray);
  const freeSpaceStart = getEntryPointerIndex(entryCount + 1);
  if (entryCount === 0 && pageArray[0] === 0) {
    // not initialized, so we have to pretend it was initialized
    return pageArray.length - freeSpaceStart;
  } else {
    const freeSpaceEnd = readUint16(pageArray, FREE_SPACE_END_POINTER);
    return freeSpaceEnd - freeSpaceStart + readUint16(pageArray, FREE_CHUNKS_SIZE);
  }
}

function readEntryLength(pageArray: Uint8Array, entryPointer: number): number {
  const byte0 = pageArray[entryPointer];
  if (byte0 <= 0x7f) {
    return byte0;
  } else {
    return ((byte0 & 0x7f) << 8) | pageArray[entryPointer + 1];
  }
}

function getByteCountForEntryLength(entryLength: number): number {
  return entryLength <= 0x7f ? 1 : 2;
}

const SHARED_EMPTY_ARRAY = new Uint8Array(0);

function readEntry(pageArray: Uint8Array, entryCount: number, entryNumber: number): Uint8Array {
  if (entryNumber < 0 || entryNumber >= entryCount) {
    throw new Error("invalid entryNumber: " + entryNumber);
  }
  const headerPointer = readUint16(pageArray, getEntryPointerIndex(entryNumber));
  if (headerPointer === 0) {
    // special case for empty array
    return SHARED_EMPTY_ARRAY;
  } else {
    const entryLength = readEntryLength(pageArray, headerPointer);
    const bytesStart = headerPointer + getByteCountForEntryLength(entryLength);
    return pageArray.subarray(bytesStart, bytesStart + entryLength);
  }
}

export function readAllPageEntries(pageArray: Uint8Array): Uint8Array[] {
  const result: Uint8Array[] = [];
  const entryCount = readPageEntriesCount(pageArray);
  for (let i = 0; i < entryCount; i++) {
    result.push(readEntry(pageArray, entryCount, i));
  }
  return result;
}

export function readPageEntryByNumber(pageArray: Uint8Array, entryNumber: number): Uint8Array {
  const entryCount = readPageEntriesCount(pageArray);
  return readEntry(pageArray, entryCount, entryNumber);
}

/**
 * @returns an array containing the entryNumber where the entry either exists or would be inserted and whether it exists
 */
function findEntryNumber(pageArray: Uint8Array, entryCount: number, entry: Uint8Array): [number, boolean] {
  if (entryCount <= 0) {
    return [0, false];
  }
  // binary search
  let left = 0;
  let right = entryCount - 1;

  while (right >= left) {
    const currentEntryNumber = (left + right) >> 1;
    const currentEntry = readEntry(pageArray, entryCount, currentEntryNumber);
    const compareResult = compareUint8Arrays(entry, currentEntry);
    if (compareResult === 0) {
      // found the entry
      return [currentEntryNumber, true];
    }
    if (
      left === right ||
      (left === currentEntryNumber && compareResult < 0) ||
      (currentEntryNumber === right && compareResult > 0)
    ) {
      // if entry is smaller, then insert at the current entryNumber, otherwise after it
      return [currentEntryNumber + (compareResult > 0 ? 1 : 0), false];
    }
    if (compareResult < 0) {
      right = currentEntryNumber - 1;
    } else {
      left = currentEntryNumber + 1;
    }
  }

  throw new Error("findEntryNumber did not find an entryNumber");
}

export function getEntryNumberOfPageEntry(pageArray: Uint8Array, entry: Uint8Array): number | undefined {
  const entryCount = readPageEntriesCount(pageArray);
  const [entryNumber, exists] = findEntryNumber(pageArray, entryCount, entry);
  return exists ? entryNumber : undefined;
}

export function containsPageEntry(pageArray: Uint8Array, entry: Uint8Array): boolean {
  return getEntryNumberOfPageEntry(pageArray, entry) !== undefined;
}

function scan(
  pageArray: Uint8Array,
  entryCount: number,
  startEntryNumber: number,
  direction: -1 | 1,
  callback: (entry: Uint8Array, entryNumber: number) => boolean
): void {
  for (let entryNumber = startEntryNumber; entryNumber >= 0 && entryNumber < entryCount; entryNumber += direction) {
    const entry = readEntry(pageArray, entryCount, entryNumber);
    if (!callback(entry, entryNumber)) {
      break;
    }
  }
}

export function scanPageEntries(
  pageArray: Uint8Array,
  startEntryOrEntryNumber: Uint8Array | number | undefined,
  callback: (entry: Uint8Array, entryNumber: number) => boolean
): void {
  const entryCount = readPageEntriesCount(pageArray);
  let startEntryNumber: number;
  if (startEntryOrEntryNumber === undefined) {
    startEntryNumber = 0;
  } else if (typeof startEntryOrEntryNumber === "number") {
    startEntryNumber = startEntryOrEntryNumber;
  } else {
    const [entryNumber, _] = findEntryNumber(pageArray, entryCount, startEntryOrEntryNumber);
    // in the forward scan case the returned entryNumber is always the right one
    startEntryNumber = entryNumber;
  }

  scan(pageArray, entryCount, startEntryNumber, 1, callback);
}

export function scanPageEntriesReverse(
  pageArray: Uint8Array,
  startEntryOrEntryNumber: Uint8Array | number | undefined,
  callback: (entry: Uint8Array, entryNumber: number) => boolean
): void {
  const entryCount = readPageEntriesCount(pageArray);
  let startEntryNumber: number;
  if (startEntryOrEntryNumber === undefined) {
    startEntryNumber = entryCount - 1;
  } else if (typeof startEntryOrEntryNumber === "number") {
    startEntryNumber = startEntryOrEntryNumber;
  } else {
    const [entryNumber, exists] = findEntryNumber(pageArray, entryCount, startEntryOrEntryNumber);
    startEntryNumber = exists ? entryNumber : entryNumber - 1;
  }

  scan(pageArray, entryCount, startEntryNumber, -1, callback);
}

function initIfNecessary(pageArray: Uint8Array): void {
  if (pageArray[0] === 0) {
    pageArray[0] = 1;
    writeUint16(pageArray, FREE_SPACE_END_POINTER, pageArray.length);
    writeUint16(pageArray, FREE_CHUNKS_SIZE, 0);
    writeUint16(pageArray, ENTRY_COUNT, 0);
  }
}

function getSortedEntryPointers(pageArray: Uint8Array, entryCount: number): number[] {
  const result: number[] = [];

  for (let i = 0; i < entryCount; i++) {
    const entryPointer = readUint16(pageArray, getEntryPointerIndex(i));
    if (entryPointer > 0) {
      result.push(entryPointer);
    }
  }

  if (entryCount >= 2) {
    // we need to sort
    result.sort((a, b) => a - b);
  }
  return result;
}

function findFreeChunk(
  pageArray: Uint8Array,
  entryCount: number,
  freeChunksSize: number,
  requiredLength: number
): number | undefined {
  if (freeChunksSize < requiredLength) {
    return undefined;
  }
  const sortedEntryPointers = getSortedEntryPointers(pageArray, entryCount);
  const pointersLength = sortedEntryPointers.length;

  let remainingFreeChunksSize = freeChunksSize;
  let candidatePointer: number | undefined = undefined;
  let candidateLength: number | undefined = undefined;

  for (let i = 0; i < pointersLength && remainingFreeChunksSize >= requiredLength; i++) {
    const entryPointer = sortedEntryPointers[i];
    const entryLength = readEntryLength(pageArray, entryPointer);
    const entryEnd = entryPointer + getByteCountForEntryLength(entryLength) + entryLength;
    let nextEntryPointer: number;
    if (i + 1 < pointersLength) {
      nextEntryPointer = sortedEntryPointers[i + 1];
    } else {
      // there might be a gap after the last entry
      nextEntryPointer = pageArray.length;
    }
    const freeChunkLength = nextEntryPointer - entryEnd;
    if (freeChunkLength === requiredLength) {
      // exact match, just return
      return entryEnd;
    }
    // use "<=" to prefer later free chunks to fill those first, making the search for future calls potentially faster
    if (freeChunkLength > requiredLength && (candidateLength === undefined || freeChunkLength <= candidateLength)) {
      candidateLength = freeChunkLength;
      candidatePointer = entryEnd;
    }
    remainingFreeChunksSize -= freeChunkLength;
  }

  return candidatePointer;
}

function tryWriteEntry(pageArray: Uint8Array, entryCount: number, entry: Uint8Array): number | undefined {
  // allow one extra space for the new entry entry
  const freeSpaceStart = getEntryPointerIndex(entryCount + 1);
  const freeSpaceEnd = readUint16(pageArray, FREE_SPACE_END_POINTER);
  if (freeSpaceStart > freeSpaceEnd) {
    // there is no space for another entry in the entries array
    return undefined;
  }

  const entryLength = entry.length;
  const byteCountForEntryLength = getByteCountForEntryLength(entryLength);
  const usedBytes = entryLength + byteCountForEntryLength;

  const freeChunksSize = readUint16(pageArray, FREE_CHUNKS_SIZE);

  let newEntryPointer = findFreeChunk(pageArray, entryCount, freeChunksSize, usedBytes);

  if (newEntryPointer !== undefined) {
    // we use a free chunk
    writeUint16(pageArray, FREE_CHUNKS_SIZE, freeChunksSize - usedBytes);
  } else {
    // free chunks were not sufficient, so use the free space if possible
    newEntryPointer = freeSpaceEnd - usedBytes;
    if (newEntryPointer < freeSpaceStart) {
      // not enough space
      return undefined;
    }
    writeUint16(pageArray, FREE_SPACE_END_POINTER, newEntryPointer);
  }

  if (newEntryPointer !== undefined) {
    if (byteCountForEntryLength === 1) {
      pageArray[newEntryPointer] = entryLength;
    } else {
      pageArray[newEntryPointer] = 0x80 | (entryLength >>> 8);
      pageArray[newEntryPointer + 1] = entryLength & 0xff;
    }
    pageArray.set(entry, newEntryPointer + byteCountForEntryLength);
  }
  return newEntryPointer;
}

/**
 * @returns whether the insert was successful (i.e. there was sufficient space), if the the entry already existed, then
 *          true is also returned
 */
export function insertPageEntry(pageArray: Uint8Array, entry: Uint8Array): boolean {
  if (entry.length > MAX_ENTRY_LENGTH) {
    throw new Error("entry is too long");
  }
  const entryCount = readPageEntriesCount(pageArray);
  const [entryNumber, exists] = findEntryNumber(pageArray, entryCount, entry);
  if (exists) {
    // entry already exists
    return true;
  }
  // we need to insert
  if (entryCount === 0) {
    initIfNecessary(pageArray);
  }
  let headerPointer: number;
  if (entry.length === 0) {
    // just use 0 as header pointer
    headerPointer = 0;
  } else {
    let writeResult = tryWriteEntry(pageArray, entryCount, entry);
    if (writeResult === undefined) {
      // could not write
      return false;
    }

    headerPointer = writeResult;
  }
  // shift entries before inserting
  for (let i = entryCount; i > entryNumber; i--) {
    const base = getEntryPointerIndex(i);
    pageArray[base] = pageArray[base - 2];
    pageArray[base + 1] = pageArray[base - 1];
  }
  // write the headerPointer
  writeUint16(pageArray, getEntryPointerIndex(entryNumber), headerPointer);
  // and increase the count
  writeUint16(pageArray, ENTRY_COUNT, entryCount + 1);
  return true;
}

/**
 * @returns whether the entry existed
 */
export function removePageEntry(pageArray: Uint8Array, entry: Uint8Array): boolean {
  if (entry.length > MAX_ENTRY_LENGTH) {
    throw new Error("entry is too long");
  }
  const entryCount = readPageEntriesCount(pageArray);
  const [entryNumber, exists] = findEntryNumber(pageArray, entryCount, entry);
  if (!exists) {
    // entry does not exist
    return false;
  }

  const entryPointer = readUint16(pageArray, getEntryPointerIndex(entryNumber));
  // shift all the following entries backwards in the array
  for (let i = entryNumber + 1; i < entryCount; i++) {
    const base = getEntryPointerIndex(i);
    pageArray[base - 2] = pageArray[base];
    pageArray[base - 1] = pageArray[base + 1];
  }
  // and decrease the count
  const newEntryCount = entryCount - 1;
  writeUint16(pageArray, ENTRY_COUNT, newEntryCount);

  if (entryPointer) {
    // we potentially need to update the free space end pointer and free chunks size
    const oldFreeSpaceEnd = readUint16(pageArray, FREE_SPACE_END_POINTER);
    const oldFreeChunksSize = readUint16(pageArray, FREE_CHUNKS_SIZE);
    const entryLength = readEntryLength(pageArray, entryPointer);
    const entryTotalLength = entryLength + getByteCountForEntryLength(entryLength);

    if (oldFreeSpaceEnd === entryPointer) {
      const minNewFreeSpaceEnd = oldFreeSpaceEnd + entryTotalLength;
      let newFreeSpaceEnd = pageArray.length;

      for (let i = 0; i < newEntryCount && minNewFreeSpaceEnd < newFreeSpaceEnd; i++) {
        const entryPointer = readUint16(pageArray, getEntryPointerIndex(i));
        if (entryPointer > 0 && entryPointer < newFreeSpaceEnd) {
          newFreeSpaceEnd = entryPointer;
        }
      }
      assert(newFreeSpaceEnd >= minNewFreeSpaceEnd);

      writeUint16(pageArray, FREE_SPACE_END_POINTER, newFreeSpaceEnd);
      const removedFreeChunksSize = newFreeSpaceEnd - oldFreeSpaceEnd - entryTotalLength;
      if (removedFreeChunksSize > 0) {
        const newFreeChunksSize = oldFreeChunksSize - removedFreeChunksSize;
        assert(newFreeChunksSize >= 0);
        writeUint16(pageArray, FREE_CHUNKS_SIZE, newFreeChunksSize);
      } else {
        assert(removedFreeChunksSize === 0);
      }
    } else {
      const newFreeChunksSize = oldFreeChunksSize + entryTotalLength;
      writeUint16(pageArray, FREE_CHUNKS_SIZE, newFreeChunksSize);
    }
  }

  return true;
}

export function debugPageEntries(pageArray: Uint8Array): [number, number, string][] {
  const entryCount = readPageEntriesCount(pageArray);
  if (entryCount === 0 && pageArray[0] === 0) {
    return [];
  }
  const result: [number, number, string][] = [];
  const freeSpaceStart = getEntryPointerIndex(entryCount);
  const freeSpaceEnd = readUint16(pageArray, FREE_SPACE_END_POINTER);
  result.push([freeSpaceStart, freeSpaceEnd, "free space"]);

  for (let i = 0; i < entryCount; i++) {
    const headerPointer = readUint16(pageArray, getEntryPointerIndex(i));
    const entryLength = readEntryLength(pageArray, headerPointer);
    const bytesStart = headerPointer + getByteCountForEntryLength(entryLength);
    result.push([headerPointer, bytesStart + entryLength, "entry chunk " + i]);
  }

  result.sort((a, b) => a[0] - b[0]);
  return result;
}
