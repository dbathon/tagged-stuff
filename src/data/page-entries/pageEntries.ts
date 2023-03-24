import { compareUint8Arrays } from "./compareUint8Arrays";

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
 * Entry pointers point to a header of the first (of potentially multiple) chunks of bytes. The header is two bytes (16 bit):
 *   bit f: 0: last chunk of entry, 1: there are more chunks (pointed to by the two bytes after the header)
 *   bit e: 0: use data of this chunk, 1: use prefix of entry before or after (see next bit)
 *   bit d: 0: use prefix of entry before, 1: use prefix of entry after,
 *   bit c and b: reserved
 *   bit a to 0: length
 *
 * TODO: the implementation is not optimized for now...
 * TODO: implement prefixes during writing
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
const FREE_CHUNKS_POINTER = 3;
const FREE_CHUNKS_SIZE = 5;
const ENTRY_COUNT = 7;
const ENTRIES = 9;

// entry header masks
const HEADER_MORE_CHUNKS = 0b1000_0000_0000_0000;
const HEADER_USE_PREFIX = 0b0100_0000_0000_0000;
const HEADER_USE_PREFIX_AFTER = 0b0010_0000_0000_0000;
const HEADER_LENGTH = 0b0000_0111_1111_1111;

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

function concat(array1: Uint8Array, array2: Uint8Array): Uint8Array {
  const result = new Uint8Array(array1.length + array2.length);
  result.set(array1, 0);
  result.set(array2, array1.length);
  return result;
}

function readLengthBytesStartAndNextHeaderPointer(
  pageArray: Uint8Array,
  headerPointer: number
): [number, number, number | undefined] {
  const header = readUint16(pageArray, headerPointer);
  const moreChunks = (header & HEADER_MORE_CHUNKS) !== 0;
  const length = header & HEADER_LENGTH;
  const bytesStart = headerPointer + (moreChunks ? 4 : 2);
  const nextHeaderPointer = moreChunks ? readUint16(pageArray, headerPointer + 2) : undefined;
  return [length, bytesStart, nextHeaderPointer];
}

function readUsePrefix(pageArray: Uint8Array, headerPointer: number): boolean {
  return (pageArray[headerPointer] & HEADER_USE_PREFIX) !== 0;
}

function readChunks(pageArray: Uint8Array, headerPointer: number): Uint8Array {
  let currentHeaderPointer = headerPointer;
  let result: Uint8Array | undefined = undefined;
  while (true) {
    if (readUsePrefix(pageArray, currentHeaderPointer)) {
      throw new Error("readChunks does not support prefixes");
    }
    const [length, bytesStart, nextHeaderPointer] = readLengthBytesStartAndNextHeaderPointer(
      pageArray,
      currentHeaderPointer
    );
    const chunk = pageArray.subarray(bytesStart, bytesStart + length);
    result = result ? concat(result, chunk) : chunk;
    if (nextHeaderPointer === undefined) {
      return result;
    }
    currentHeaderPointer = nextHeaderPointer;
  }
}

const SHARED_EMPTY_ARRAY = new Uint8Array(0);

function readEntry(
  pageArray: Uint8Array,
  entryCount: number,
  entryNumber: number,
  entryCache: Uint8Array[]
): Uint8Array {
  if (entryNumber < 0 || entryNumber >= entryCount) {
    throw new Error("invalid entryNumber: " + entryNumber);
  }
  const cachedResult = entryCache[entryNumber];
  if (cachedResult) {
    return cachedResult;
  }
  const headerPointer = readUint16(pageArray, getEntryPointerIndex(entryNumber));
  let result: Uint8Array;
  if (headerPointer === 0) {
    // special case for empty array
    result = SHARED_EMPTY_ARRAY;
  } else {
    if (readUsePrefix(pageArray, headerPointer)) {
      const prefixAfter = (pageArray[headerPointer] & HEADER_USE_PREFIX_AFTER) !== 0;
      const [length, _, nextHeaderPointer] = readLengthBytesStartAndNextHeaderPointer(pageArray, headerPointer);
      // TODO: improve this by not reading full entries where possible
      const otherEntry = readEntry(pageArray, entryCount, entryNumber + (prefixAfter ? 1 : -1), entryCache);
      if (otherEntry.length < length) {
        throw new Error("otherEntry is too short for prefix length: " + otherEntry.length + ", " + length);
      }
      const prefixChunk = otherEntry.subarray(0, length);
      if (nextHeaderPointer !== undefined) {
        result = concat(prefixChunk, readChunks(pageArray, nextHeaderPointer));
      } else {
        result = prefixChunk;
      }
    } else {
      result = readChunks(pageArray, headerPointer);
    }
  }
  entryCache[entryNumber] = result;
  return result;
}

export function readAllPageEntries(pageArray: Uint8Array): Uint8Array[] {
  const result: Uint8Array[] = [];
  const entryCount = readPageEntriesCount(pageArray);
  const entryCache: Uint8Array[] = [];
  for (let i = 0; i < entryCount; i++) {
    result.push(readEntry(pageArray, entryCount, i, entryCache));
  }
  return result;
}

export function readPageEntryByNumber(pageArray: Uint8Array, entryNumber: number): Uint8Array {
  const entryCount = readPageEntriesCount(pageArray);
  const entryCache: Uint8Array[] = [];
  return readEntry(pageArray, entryCount, entryNumber, entryCache);
}

/**
 * @returns an array containing the entryNumber where the entry either exists or would be inserted and whether it exists
 */
function findEntryNumber(
  pageArray: Uint8Array,
  entryCount: number,
  entry: Uint8Array,
  entryCache: Uint8Array[]
): [number, boolean] {
  if (entryCount <= 0) {
    return [0, false];
  }
  // binary search
  let left = 0;
  let right = entryCount - 1;

  while (right >= left) {
    const currentEntryNumber = (left + right) >> 1;
    const currentEntry = readEntry(pageArray, entryCount, currentEntryNumber, entryCache);
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
  const entryCache: Uint8Array[] = [];
  const [entryNumber, exists] = findEntryNumber(pageArray, entryCount, entry, entryCache);
  return exists ? entryNumber : undefined;
}

export function containsPageEntry(pageArray: Uint8Array, entry: Uint8Array): boolean {
  return getEntryNumberOfPageEntry(pageArray, entry) !== undefined;
}

function scan(
  pageArray: Uint8Array,
  entryCount: number,
  entryCache: Uint8Array[],
  startEntryNumber: number,
  direction: -1 | 1,
  callback: (entry: Uint8Array, entryNumber: number) => boolean
): void {
  for (let entryNumber = startEntryNumber; entryNumber >= 0 && entryNumber < entryCount; entryNumber += direction) {
    const entry = readEntry(pageArray, entryCount, entryNumber, entryCache);
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
  const entryCache: Uint8Array[] = [];
  let startEntryNumber: number;
  if (startEntryOrEntryNumber === undefined) {
    startEntryNumber = 0;
  } else if (typeof startEntryOrEntryNumber === "number") {
    startEntryNumber = startEntryOrEntryNumber;
  } else {
    const [entryNumber, _] = findEntryNumber(pageArray, entryCount, startEntryOrEntryNumber, entryCache);
    // in the forward scan case the returned entryNumber is always the right one
    startEntryNumber = entryNumber;
  }

  scan(pageArray, entryCount, entryCache, startEntryNumber, 1, callback);
}

export function scanPageEntriesReverse(
  pageArray: Uint8Array,
  startEntryOrEntryNumber: Uint8Array | number | undefined,
  callback: (entry: Uint8Array, entryNumber: number) => boolean
): void {
  const entryCount = readPageEntriesCount(pageArray);
  const entryCache: Uint8Array[] = [];
  let startEntryNumber: number;
  if (startEntryOrEntryNumber === undefined) {
    startEntryNumber = entryCount - 1;
  } else if (typeof startEntryOrEntryNumber === "number") {
    startEntryNumber = startEntryOrEntryNumber;
  } else {
    const [entryNumber, exists] = findEntryNumber(pageArray, entryCount, startEntryOrEntryNumber, entryCache);
    startEntryNumber = exists ? entryNumber : entryNumber - 1;
  }

  scan(pageArray, entryCount, entryCache, startEntryNumber, -1, callback);
}

function initIfNecessary(pageArray: Uint8Array): void {
  if (pageArray[0] === 0) {
    pageArray[0] = 1;
    writeUint16(pageArray, FREE_SPACE_END_POINTER, pageArray.length);
    writeUint16(pageArray, FREE_CHUNKS_POINTER, 0);
    writeUint16(pageArray, FREE_CHUNKS_SIZE, 0);
    writeUint16(pageArray, ENTRY_COUNT, 0);
  }
}

const FREE_CHUNK_MIN_LENGTH = 3;
const FREE_CHUNK_MAX_LENGTH = 0b0111_1111_1111_1111;
const FREE_CHUNK_MAX_LENGTH_ONE_BYTE_MASK = 0b1000_0000;
const FREE_CHUNK_MAX_LENGTH_ONE_BYTE = 0b0111_1111;

function writeFreeChunkLengthAndNext(
  pageArray: Uint8Array,
  chunkPointer: number,
  length: number,
  nextChunkPointer: number
): void {
  if (length < FREE_CHUNK_MIN_LENGTH) {
    throw new Error("chunk too small: " + length);
  }
  if (length > FREE_CHUNK_MAX_LENGTH) {
    // chain the chunks
    const chainedChunkPointer = chunkPointer + FREE_CHUNK_MAX_LENGTH;
    writeFreeChunkLengthAndNext(pageArray, chunkPointer, FREE_CHUNK_MAX_LENGTH, chainedChunkPointer);
    writeFreeChunkLengthAndNext(pageArray, chainedChunkPointer, length - FREE_CHUNK_MAX_LENGTH, nextChunkPointer);
  } else if (length <= FREE_CHUNK_MAX_LENGTH_ONE_BYTE) {
    pageArray[chunkPointer] = length | FREE_CHUNK_MAX_LENGTH_ONE_BYTE_MASK;
    writeUint16(pageArray, chunkPointer + 1, nextChunkPointer);
  } else {
    writeUint16(pageArray, chunkPointer, length);
    writeUint16(pageArray, chunkPointer + 2, nextChunkPointer);
  }
}

function isOneByteHeaderFreeChunk(freeChunkByte1: number) {
  return (freeChunkByte1 & FREE_CHUNK_MAX_LENGTH_ONE_BYTE_MASK) !== 0;
}

function readFreeChunkLengthAndNext(pageArray: Uint8Array, chunkPointer: number): [number, number] {
  const byte1 = pageArray[chunkPointer];
  const oneByteLength = isOneByteHeaderFreeChunk(byte1);
  const length = oneByteLength ? byte1 & FREE_CHUNK_MAX_LENGTH_ONE_BYTE : readUint16(pageArray, chunkPointer);
  const nextChunkPointer: number = readUint16(pageArray, chunkPointer + (oneByteLength ? 1 : 2));
  return [length, nextChunkPointer];
}

function tryWriteEntry(
  pageArray: Uint8Array,
  entryCount: number,
  entry: Uint8Array,
  onlyOneExactFreeChunkAllowed: boolean
): number | undefined {
  // allow one extra space for the new entry entry
  const freeSpaceStart = getEntryPointerIndex(entryCount + 1);
  const freeSpaceEnd = readUint16(pageArray, FREE_SPACE_END_POINTER);
  if (freeSpaceStart > freeSpaceEnd) {
    // there is no space for another entry in the entries array
    return undefined;
  }
  const writeTasks: (() => void)[] = [];
  let rest = entry;

  let entryPointer: number | undefined = undefined;
  let previousNextChunkPointerIndex: number | undefined = undefined;
  function handleChunkPointer(chunkPointer: number): number {
    if (previousNextChunkPointerIndex !== undefined) {
      writeUint16(pageArray, previousNextChunkPointerIndex, chunkPointer);
      previousNextChunkPointerIndex = undefined;
    }
    if (entryPointer === undefined) {
      // first chunk
      entryPointer = chunkPointer;
    }
    return entryPointer;
  }

  function useFreeChunksSize(usedFreeChunksSize: number): void {
    const oldFreeChunksSize = readUint16(pageArray, FREE_CHUNKS_SIZE);
    if (usedFreeChunksSize > oldFreeChunksSize) {
      // should never happen
      throw new Error("usedFreeChunksSize > oldFreeChunksSize: " + usedFreeChunksSize + ", " + oldFreeChunksSize);
    }
    writeUint16(pageArray, FREE_CHUNKS_SIZE, oldFreeChunksSize - usedFreeChunksSize);
  }

  let previousNextFreeChunkPointerIndex = FREE_CHUNKS_POINTER;
  let currentChunkPointer = readUint16(pageArray, FREE_CHUNKS_POINTER);
  while (currentChunkPointer > 0) {
    const [freeChunkLength, nextFreeChunkPointer] = readFreeChunkLengthAndNext(pageArray, currentChunkPointer);
    const restLength = rest.length;
    const usedBytesIfLastChunk = restLength + 2;
    const remainingChunkLengthIfLastChunk = freeChunkLength - usedBytesIfLastChunk;
    if (
      remainingChunkLengthIfLastChunk === 0 ||
      (!onlyOneExactFreeChunkAllowed && remainingChunkLengthIfLastChunk >= FREE_CHUNK_MIN_LENGTH)
    ) {
      // it is the last chunk
      // execute potential previous writes
      writeTasks.forEach((task) => task());

      writeUint16(pageArray, currentChunkPointer, restLength);
      pageArray.set(rest, currentChunkPointer + 2);

      useFreeChunksSize(usedBytesIfLastChunk);

      if (remainingChunkLengthIfLastChunk > 0) {
        // create a new chunk with the remaining bytes
        const newChunkPointer = currentChunkPointer + usedBytesIfLastChunk;
        writeFreeChunkLengthAndNext(pageArray, newChunkPointer, remainingChunkLengthIfLastChunk, nextFreeChunkPointer);
        writeUint16(pageArray, previousNextFreeChunkPointerIndex, newChunkPointer);
      } else {
        writeUint16(pageArray, previousNextFreeChunkPointerIndex, nextFreeChunkPointer);
      }

      return handleChunkPointer(currentChunkPointer);
    } else if (
      !onlyOneExactFreeChunkAllowed &&
      freeChunkLength > 4 /* we need to write at least one byte */ &&
      rest.length > freeChunkLength - 4 /* but we cannot write all bytes and then a next pointer */
    ) {
      // write as much as possible
      const bytesToWriteLength = freeChunkLength - 4;
      const bytesToWrite = rest.subarray(0, bytesToWriteLength);
      const chunkPointerForTask = currentChunkPointer;
      writeTasks.push(() => {
        writeUint16(pageArray, chunkPointerForTask, HEADER_MORE_CHUNKS | bytesToWriteLength);
        pageArray.set(bytesToWrite, chunkPointerForTask + 4);

        useFreeChunksSize(freeChunkLength);

        handleChunkPointer(chunkPointerForTask);
        previousNextChunkPointerIndex = chunkPointerForTask + 2;
      });
      rest = rest.subarray(bytesToWriteLength);
    } else {
      // this chunk cannot be used in this case
      previousNextFreeChunkPointerIndex =
        currentChunkPointer + (isOneByteHeaderFreeChunk(pageArray[currentChunkPointer]) ? 1 : 2);
    }

    currentChunkPointer = nextFreeChunkPointer;
  }

  // free chunks were not sufficient, so use the free space if possible
  {
    const restLength = rest.length;
    if (restLength === 0) {
      throw new Error("rest is empty after free chunks loop");
    }
    currentChunkPointer = freeSpaceEnd - 2 - restLength;
    if (currentChunkPointer < freeSpaceStart) {
      // not enough space
      return undefined;
    }
    if (onlyOneExactFreeChunkAllowed) {
      // only use a chunk from the free space in this case if there is more free space than free chunks
      // this avoids unnecessarily removing space for further entries in the entries array
      if (readUint16(pageArray, FREE_CHUNKS_SIZE) > freeSpaceEnd - freeSpaceStart) {
        return undefined;
      }
    }

    // execute potential previous writes
    writeTasks.forEach((task) => task());

    writeUint16(pageArray, currentChunkPointer, restLength);
    pageArray.set(rest, currentChunkPointer + 2);

    writeUint16(pageArray, previousNextFreeChunkPointerIndex, 0);
    writeUint16(pageArray, FREE_SPACE_END_POINTER, currentChunkPointer);

    return handleChunkPointer(currentChunkPointer);
  }
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
  const entryCache: Uint8Array[] = [];
  const [entryNumber, exists] = findEntryNumber(pageArray, entryCount, entry, entryCache);
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
    // first try to write it in one chunk (either one free chunk or in the free space)
    let writeResult = tryWriteEntry(pageArray, entryCount, entry, true);
    if (writeResult === undefined) {
      // if one chunk is not possible, then try with multiple or partial chunks
      writeResult = tryWriteEntry(pageArray, entryCount, entry, false);
    }
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

class FreeChunkInfo {
  obsolete?: boolean;

  constructor(readonly startIndex: number, public length: number) {}

  get endIndex(): number {
    return this.startIndex + this.length;
  }
}

function readFreeChunkInfos(pageArray: Uint8Array): FreeChunkInfo[] {
  const result: FreeChunkInfo[] = [];
  let currentChunkPointer = readUint16(pageArray, FREE_CHUNKS_POINTER);
  while (currentChunkPointer > 0) {
    const [length, nextChunkPointer] = readFreeChunkLengthAndNext(pageArray, currentChunkPointer);
    result.push(new FreeChunkInfo(currentChunkPointer, length));
    currentChunkPointer = nextChunkPointer;
  }
  return result;
}

function mergeFreeChunkInfos(freeChunkInfos: FreeChunkInfo[]): void {
  const sortedInfos = [...freeChunkInfos].sort((a, b) => a.startIndex - b.startIndex);
  let previous: FreeChunkInfo | undefined = undefined;
  for (const freeChunkInfo of sortedInfos) {
    if (previous && previous.endIndex === freeChunkInfo.startIndex) {
      // merge the two free chunks
      previous.length += freeChunkInfo.length;
      freeChunkInfo.obsolete = true;
    } else {
      previous = freeChunkInfo;
    }
  }
}

function writeFreeChunkInfos(pageArray: Uint8Array, freeChunkInfos: FreeChunkInfo[]): void {
  let previous: FreeChunkInfo | undefined = undefined;
  let freeChunksSize = 0;
  for (const freeChunkInfo of freeChunkInfos) {
    if (freeChunkInfo.obsolete) {
      // do not write obsolete ones
      continue;
    }
    freeChunksSize += freeChunkInfo.length;
    if (!previous) {
      // first iteration
      writeUint16(pageArray, FREE_CHUNKS_POINTER, freeChunkInfo.startIndex);
    } else {
      // write previous
      writeFreeChunkLengthAndNext(pageArray, previous.startIndex, previous.length, freeChunkInfo.startIndex);
    }
    previous = freeChunkInfo;
  }

  if (!previous) {
    // freeChunkInfos was empty
    writeUint16(pageArray, FREE_CHUNKS_POINTER, 0);
  } else {
    // write the last chunk
    writeFreeChunkLengthAndNext(pageArray, previous.startIndex, previous.length, 0);
  }

  writeUint16(pageArray, FREE_CHUNKS_SIZE, freeChunksSize);
}

/**
 * @returns whether the entry existed
 */
export function removePageEntry(pageArray: Uint8Array, entry: Uint8Array): boolean {
  if (entry.length > MAX_ENTRY_LENGTH) {
    throw new Error("entry is too long");
  }
  const entryCount = readPageEntriesCount(pageArray);
  const entryCache: Uint8Array[] = [];
  const [entryNumber, exists] = findEntryNumber(pageArray, entryCount, entry, entryCache);
  if (!exists) {
    // entry does not exist
    return false;
  }

  let chunkPointer: number | undefined = readUint16(pageArray, getEntryPointerIndex(entryNumber));
  // shift all the following entries backwards in the array
  for (let i = entryNumber + 1; i < entryCount; i++) {
    const base = getEntryPointerIndex(i);
    pageArray[base - 2] = pageArray[base];
    pageArray[base - 1] = pageArray[base + 1];
  }
  // and decrease the count
  writeUint16(pageArray, ENTRY_COUNT, entryCount - 1);

  if (chunkPointer) {
    // add the new free chunks to the free chunk list

    // we need to read all the free chunk infos, since we want to merge them if possible
    const freeChunkInfos = readFreeChunkInfos(pageArray);

    let result: Uint8Array;
    while (chunkPointer !== undefined) {
      const [length, _, nextHeaderPointer] = readLengthBytesStartAndNextHeaderPointer(pageArray, chunkPointer);
      const isPrefixChunk = readUsePrefix(pageArray, chunkPointer);
      const freeChunkLength = 2 + (nextHeaderPointer !== undefined ? 2 : 0) + (isPrefixChunk ? 0 : length);
      freeChunkInfos.push(new FreeChunkInfo(chunkPointer, freeChunkLength));

      chunkPointer = nextHeaderPointer;
    }

    mergeFreeChunkInfos(freeChunkInfos);

    // check if we can merge one of the chunks with the free space end pointer
    const freeSpaceEnd = readUint16(pageArray, FREE_SPACE_END_POINTER);
    for (const freeChunkInfo of freeChunkInfos) {
      if (!freeChunkInfo.obsolete && freeChunkInfo.startIndex === freeSpaceEnd) {
        freeChunkInfo.obsolete = true;
        writeUint16(pageArray, FREE_SPACE_END_POINTER, freeSpaceEnd + freeChunkInfo.length);
        // there can be no other chunks, since we already merged them
        break;
      }
    }

    // write all of them again for simplicity, for chunks that did not change nothing will change in the array
    writeFreeChunkInfos(pageArray, freeChunkInfos);
  }

  return true;
}

export function debugPageEntriesChunks(pageArray: Uint8Array): [number, number, string][] {
  const entryCount = readPageEntriesCount(pageArray);
  if (entryCount === 0 && pageArray[0] === 0) {
    return [];
  }
  const result: [number, number, string][] = [];
  const freeSpaceStart = getEntryPointerIndex(entryCount);
  const freeSpaceEnd = readUint16(pageArray, FREE_SPACE_END_POINTER);
  result.push([freeSpaceStart, freeSpaceEnd, "free space"]);
  readFreeChunkInfos(pageArray).forEach((info, index) =>
    result.push([info.startIndex, info.endIndex, "free chunk " + index])
  );
  for (let i = 0; i < entryCount; i++) {
    let headerPointer: number | undefined = readUint16(pageArray, getEntryPointerIndex(i));
    let chunkIndex = 0;
    while (headerPointer) {
      const [length, bytesStart, nextHeaderPointer] = readLengthBytesStartAndNextHeaderPointer(
        pageArray,
        headerPointer
      );
      if (readUsePrefix(pageArray, headerPointer)) {
        result.push([headerPointer, bytesStart, "entry chunk " + i + "-" + chunkIndex + " (prefix)"]);
      } else {
        result.push([headerPointer, bytesStart + length, "entry chunk " + i + "-" + chunkIndex]);
      }
      headerPointer = nextHeaderPointer;
      chunkIndex++;
    }
  }

  result.sort((a, b) => a[0] - b[0]);
  return result;
}
