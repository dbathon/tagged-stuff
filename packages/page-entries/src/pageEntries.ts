import { assert, compareUint8Arrays } from "shared-util";

/**
 * The functions in this file allow treating "page data" (Uint8Array) like a sorted set of byte arrays (of up to 2000
 * bytes)...
 *
 * The slightly complicated layout of the entries is to allow small diffs (bytes change "as little as possible").
 * The returned entries often share the underlying ArrayBuffer with the passed pageArray (to avoid unnecessary copies).
 * So the returned entries can change if pageArray is modified, if necessary the caller should copy the returned
 * entries.
 *
 * Page layout:
 * If the first byte is 0, then the page is just considered empty. Otherwise it is the "layout version" tag (currently
 * 1 in if there is no common prefix and 2 if there is a common prefix).
 * Free space end pointer (2 bytes): a pointer to the end (exclusive) of the free space between the entries array and
 * the data.
 * Free chunks size (2 bytes): the sum of bytes available between existing entries (does not include the "free space"
 * ending at free space end pointer).
 * Entry count (2 bytes).
 * Entry pointers sorted by entry (2 bytes each)
 *
 * Entry pointers point to the start of the entry. The first one or two bytes specify the length of the entry, after
 * the length the entry bytes follow. The entry pointer 0 is a special cases that denotes the empty entry.
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
 * page..., but also helps with not requiring too many bits for the length before the entry bytes.
 */
const MAX_ENTRY_LENGTH = 2000;

/**
 * We want to store the length in one byte.
 */
const MAX_COMMON_PREFIX_LENGTH = 0xff;

// "pointers"
const TAG = 0;
const FREE_SPACE_END_POINTER = 1;
const FREE_CHUNKS_SIZE = 3;
const ENTRY_COUNT = 5;
const ENTRIES = 7;

const TAG_EMPTY = 0;
const TAG_NO_PREFIX = 1;
const TAG_WITH_PREFIX = 2;
// TODO: maybe have multiple versions of TAG_WITH_PREFIX with different numbers of bits used for the prefix length etc.

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

function readEntryPointer(pageArray: Uint8Array, index: number): number {
  return readUint16(pageArray, getEntryPointerIndex(index));
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
  const tag = pageArray[TAG];
  if (tag === TAG_EMPTY) {
    // the page is empty
    return 0;
  }
  if (tag !== TAG_NO_PREFIX && tag !== TAG_WITH_PREFIX) {
    throw new Error("unexpected tag: " + tag);
  }
  return readUint16(pageArray, ENTRY_COUNT);
}

export function resetPageEntries(pageArray: Uint8Array): void {
  checkPageArraySize(pageArray);
  // just set the tag to TAG_EMPTY (see readPageEntriesCount()
  pageArray[TAG] = TAG_EMPTY;
}

function hasCommonPrefix(pageArray: Uint8Array): boolean {
  return pageArray[TAG] === TAG_WITH_PREFIX;
}

function readCommonPrefix(pageArray: Uint8Array, length?: number): Uint8Array | undefined {
  if (hasCommonPrefix(pageArray)) {
    const lengthIndex = pageArray.length - 1;
    const prefixLength = pageArray[lengthIndex];
    const startIndex = lengthIndex - prefixLength;
    const usedLength = length ?? prefixLength;
    assert(usedLength <= prefixLength);
    return pageArray.subarray(startIndex, startIndex + usedLength);
  }
  return undefined;
}

function getMaxFreeSpaceEndPointer(pageArray: Uint8Array): number {
  if (hasCommonPrefix(pageArray)) {
    const prefixLength = pageArray[pageArray.length - 1];
    return pageArray.length - 1 - prefixLength;
  }
  return pageArray.length;
}

export function readPageEntriesFreeSpace(pageArray: Uint8Array): number {
  const entryCount = readPageEntriesCount(pageArray);
  // use +1 here to allow for the new entry that would use the free space
  const freeSpaceStart = getEntryPointerIndex(entryCount + 1);
  if (entryCount === 0 && pageArray[TAG] === TAG_EMPTY) {
    // not initialized, so we have to pretend it was initialized
    return getMaxFreeSpaceEndPointer(pageArray) - freeSpaceStart;
  } else {
    const freeSpaceEnd = readUint16(pageArray, FREE_SPACE_END_POINTER);
    return freeSpaceEnd - freeSpaceStart + readUint16(pageArray, FREE_CHUNKS_SIZE);
  }
}

function readRawEntryLength(pageArray: Uint8Array, entryPointer: number): number {
  const byte0 = pageArray[entryPointer];
  if (byte0 <= 0x7f) {
    return byte0;
  } else {
    return ((byte0 & 0x7f) << 8) | pageArray[entryPointer + 1];
  }
}

function toEntryLength(pageArray: Uint8Array, rawEntryLength: number): number {
  return pageArray[TAG] === TAG_WITH_PREFIX ? rawEntryLength >>> 2 : rawEntryLength;
}

function getByteCountForRawEntryLength(entryLength: number): number {
  return entryLength <= 0x7f ? 1 : 2;
}

function getByteCountForEntryWithLength(pageArray: Uint8Array, entryPointer: number): number {
  const rawEntryLength = readRawEntryLength(pageArray, entryPointer);
  const entryLength = toEntryLength(pageArray, rawEntryLength);
  return getByteCountForRawEntryLength(rawEntryLength) + entryLength;
}

const SHARED_EMPTY_ARRAY = new Uint8Array(0);

function readEntry(pageArray: Uint8Array, entryCount: number, entryNumber: number): Uint8Array {
  if (entryNumber < 0 || entryNumber >= entryCount) {
    throw new Error("invalid entryNumber: " + entryNumber);
  }
  const entryPointer = readEntryPointer(pageArray, entryNumber);
  if (entryPointer === 0) {
    // special case for empty array
    return SHARED_EMPTY_ARRAY;
  } else {
    const rawEntryLength = readRawEntryLength(pageArray, entryPointer);
    const bytesStart = entryPointer + getByteCountForRawEntryLength(rawEntryLength);
    const entryLength = toEntryLength(pageArray, rawEntryLength);
    const rawEntryArray = pageArray.subarray(bytesStart, bytesStart + entryLength);
    if (entryLength !== rawEntryLength) {
      // we potentially need to include a part of the prefix in the result
      // the first two bits encode how many bytes of the common prefix are used (0, 2, 4 or all of them)
      const prefixLength = (rawEntryLength & 0b11) << 1;
      if (prefixLength > 0) {
        const prefix = readCommonPrefix(pageArray, prefixLength === 6 ? undefined : prefixLength);
        assert(prefix);
        const result = new Uint8Array(prefix.length + entryLength);
        result.set(prefix);
        result.set(rawEntryArray, prefix.length);
        return result;
      }
    }
    return rawEntryArray;
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
  callback: (entry: Uint8Array, entryNumber: number) => boolean,
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
  callback: (entry: Uint8Array, entryNumber: number) => boolean,
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
  callback: (entry: Uint8Array, entryNumber: number) => boolean,
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

function initIfNecessary(pageArray: Uint8Array, commonPrefix?: Uint8Array): void {
  if (pageArray[TAG] === TAG_EMPTY) {
    let tag = TAG_NO_PREFIX;

    if (commonPrefix !== undefined) {
      const maxPrefixLength = Math.min(readPageEntriesFreeSpace(pageArray) - 10, MAX_COMMON_PREFIX_LENGTH);
      const prefixLength = commonPrefix.length;
      if (prefixLength > 0 && prefixLength <= maxPrefixLength) {
        // using the prefix is possible
        tag = TAG_WITH_PREFIX;

        // write the prefix at the end of the array
        pageArray[pageArray.length - 1] = prefixLength;
        pageArray.set(commonPrefix, pageArray.length - 1 - prefixLength);
      }
    }

    pageArray[TAG] = tag;
    writeUint16(pageArray, FREE_SPACE_END_POINTER, getMaxFreeSpaceEndPointer(pageArray));
    writeUint16(pageArray, FREE_CHUNKS_SIZE, 0);
    writeUint16(pageArray, ENTRY_COUNT, 0);
  }
}

function getSortedEntryPointers(pageArray: Uint8Array, entryCount: number): number[] {
  const result: number[] = [];

  for (let i = 0; i < entryCount; i++) {
    const entryPointer = readEntryPointer(pageArray, i);
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
  requiredLength: number,
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
    const entryEnd = entryPointer + getByteCountForEntryWithLength(pageArray, entryPointer);
    let nextEntryPointer: number;
    if (i + 1 < pointersLength) {
      nextEntryPointer = sortedEntryPointers[i + 1];
    } else {
      // there might be a gap after the last entry
      nextEntryPointer = getMaxFreeSpaceEndPointer(pageArray);
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

function findCommonPrefixLength(a: Uint8Array, b: Uint8Array): number {
  const maxLength = Math.min(a.length, b.length, MAX_COMMON_PREFIX_LENGTH);
  let prefixLength = 0;
  while (prefixLength < maxLength && a[prefixLength] === b[prefixLength]) {
    ++prefixLength;
  }
  return prefixLength;
}

function tryWriteEntry(pageArray: Uint8Array, entryCount: number, entry: Uint8Array): number | undefined {
  // allow one extra space for the new entry entry
  const freeSpaceStart = getEntryPointerIndex(entryCount + 1);
  const freeSpaceEnd = readUint16(pageArray, FREE_SPACE_END_POINTER);
  if (freeSpaceStart > freeSpaceEnd) {
    // there is no space for another entry in the entries array
    return undefined;
  }

  let rawEntryLength = entry.length;
  const commonPrefix = readCommonPrefix(pageArray);
  if (commonPrefix) {
    let prefixBits = 0;
    const matchingPrefixLength = findCommonPrefixLength(commonPrefix, entry);
    if (matchingPrefixLength >= 2) {
      let usedPrefixLength;
      if (matchingPrefixLength === commonPrefix.length) {
        usedPrefixLength = matchingPrefixLength;
        prefixBits = 3;
      } else if (matchingPrefixLength >= 4) {
        usedPrefixLength = 4;
        prefixBits = 2;
      } else {
        usedPrefixLength = 2;
        prefixBits = 1;
      }
      entry = entry.subarray(usedPrefixLength);
      rawEntryLength -= usedPrefixLength;
    }
    rawEntryLength = (rawEntryLength << 2) | prefixBits;
  }

  const byteCountForRawEntryLength = getByteCountForRawEntryLength(rawEntryLength);
  const usedBytes = byteCountForRawEntryLength + entry.length;

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

  if (byteCountForRawEntryLength === 1) {
    pageArray[newEntryPointer] = rawEntryLength;
  } else {
    pageArray[newEntryPointer] = 0x80 | (rawEntryLength >>> 8);
    pageArray[newEntryPointer + 1] = rawEntryLength & 0xff;
  }
  pageArray.set(entry, newEntryPointer + byteCountForRawEntryLength);

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
  let entryPointer: number;
  if (entry.length === 0) {
    // special case: use 0 as entry pointer
    entryPointer = 0;
  } else {
    let writeResult = tryWriteEntry(pageArray, entryCount, entry);
    if (writeResult === undefined) {
      // could not write
      return false;
    }

    entryPointer = writeResult;
  }
  // shift entries before inserting
  for (let i = entryCount; i > entryNumber; i--) {
    const base = getEntryPointerIndex(i);
    pageArray[base] = pageArray[base - 2];
    pageArray[base + 1] = pageArray[base - 1];
  }
  // write the entryPointer
  writeUint16(pageArray, getEntryPointerIndex(entryNumber), entryPointer);
  // and increase the count
  const newEntryCount = entryCount + 1;
  writeUint16(pageArray, ENTRY_COUNT, newEntryCount);

  setCommonPrefixAfterInsertIfNecessary(pageArray, newEntryCount);

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

  const newEntryCount = entryCount - 1;
  if (newEntryCount === 0) {
    // just reset the page entirely
    resetPageEntries(pageArray);
    return true;
  }

  const entryPointer = readEntryPointer(pageArray, entryNumber);
  // shift all the following entries backwards in the array
  for (let i = entryNumber + 1; i < entryCount; i++) {
    const base = getEntryPointerIndex(i);
    pageArray[base - 2] = pageArray[base];
    pageArray[base - 1] = pageArray[base + 1];
  }
  // and decrease the count
  writeUint16(pageArray, ENTRY_COUNT, newEntryCount);

  if (entryPointer) {
    // we potentially need to update the free space end pointer and free chunks size
    const oldFreeSpaceEnd = readUint16(pageArray, FREE_SPACE_END_POINTER);
    const oldFreeChunksSize = readUint16(pageArray, FREE_CHUNKS_SIZE);
    const entryTotalLength = getByteCountForEntryWithLength(pageArray, entryPointer);

    if (oldFreeSpaceEnd === entryPointer) {
      const minNewFreeSpaceEnd = oldFreeSpaceEnd + entryTotalLength;
      let newFreeSpaceEnd = getMaxFreeSpaceEndPointer(pageArray);

      for (let i = 0; i < newEntryCount && minNewFreeSpaceEnd < newFreeSpaceEnd; i++) {
        const entryPointer = readEntryPointer(pageArray, i);
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

/**
 * Sets up the common prefix if there is one for the existing entries.
 */
function setCommonPrefixAfterInsertIfNecessary(pageArray: Uint8Array, entryCount: number): void {
  if (entryCount !== 3) {
    // currently we only do this after the insert of the third entry
    return;
  }
  if (hasCommonPrefix(pageArray)) {
    // there is already a common prefix, don't try to update/modify it for now
    return;
  }

  const firstEntry = readEntry(pageArray, entryCount, 0);
  const thirdEntry = readEntry(pageArray, entryCount, 2);
  let prefixLength = findCommonPrefixLength(firstEntry, thirdEntry);

  // for now only use prefixes with at least length 2
  if (prefixLength >= 2) {
    // copy the entries, since pageArray will be modified below
    const secondEntry = readEntry(pageArray, entryCount, 1);
    const entryCopies = [new Uint8Array(firstEntry), new Uint8Array(secondEntry), new Uint8Array(thirdEntry)];

    const commonPrefix = entryCopies[0].subarray(0, prefixLength);

    // reset and setup the prefix
    resetPageEntries(pageArray);
    initIfNecessary(pageArray, commonPrefix);

    // and then insert the three entries again
    for (const entry of entryCopies) {
      assert(insertPageEntry(pageArray, entry));
    }
  }
}
