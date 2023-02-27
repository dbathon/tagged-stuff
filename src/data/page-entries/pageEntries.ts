/**
 * The functions in this file allow treating "page data" (Uint8Array) like a sorted set of byte arrays (of up to 2000 bytes)...
 * The slightly complicated layout of the entries is to allow small diffs (bytes change "as little as possible").
 *
 * Page layout:
 * If the first byte is 0, then the page is just considered empty. Otherwise it is the "layout version" (currently always 1).
 * Free space pointer (2 bytes), basically a pointer to a special "entry" (no length restriction in this case), but the bytes of the entry are all considered free space
 * Entry count (2 bytes).
 * Entry pointers sorted by entry (2 bytes each)
 *
 * Entry pointers point to a trailer of the first (of potentially multiple) chunks of bytes. The trailer can be one or two bytes:
 * first byte:
 *   first/highest bit: 0: only one byte, the length is the lowest 4 bits of this byte, 1: two bytes and the length is 12 bits
 *   second bit: 0: last chunk of entry, 1: there are more chunks (pointed to by the two bytes preceding the chunk)
 *   third and forth bit: 0: use data of this chunk, 1: reserved, 2: use prefix of entry before, 3: use prefix of entry after,
 *
 * TODO: the implementation is not optimized for now...
 */

/**
 * The maximum length of an entry. This is an arbitrary restrictions to avoid entries that take too much space of a
 * page..., but also helps with not requiring too many bits for the length in the chunk trailers.
 */
const MAX_ENTRY_LENGTH = 2000;

// "pointers"
const FREE_SPACE_POINTER = 1;
const ENTRY_COUNT = 3;
const ENTRIES = 5;

// trailer masks
const TRAILER_TWO_BYTES = 0b1000_0000;
const TRAILER_MORE_CHUNKS = 0b0100_0000;
const TRAILER_USE_PREFIX = 0b0010_0000;
const TRAILER_USE_PREFIX_AFTER = 0b0001_0000;
const TRAILER_LENGTH = 0b1111;

function readUint16(array: Uint8Array, index: number): number {
  return (array[index] << 8) | array[index + 1];
}

function writeUint16(array: Uint8Array, index: number, value: number): void {
  array[index] = (value >> 8) & 0xff;
  array[index + 1] = value & 0xff;
}

function readEntryCount(pageArray: Uint8Array): number {
  if (pageArray.length < 4000) {
    throw new Error("page is too small");
  }
  if (pageArray.length > 0xffff) {
    throw new Error("page is too large");
  }
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

function concat(array1: Uint8Array, array2: Uint8Array): Uint8Array {
  const result = new Uint8Array(array1.length + array2.length);
  result.set(array1, 0);
  result.set(array2, array1.length);
  return result;
}

function readLengthChunkEndAndNextTrailerPointer(
  pageArray: Uint8Array,
  trailerPointer: number
): [number, number, number | undefined] {
  const trailerByte1 = pageArray[trailerPointer];
  const twoBytes = (trailerByte1 & TRAILER_TWO_BYTES) !== 0;
  const moreChunks = (trailerByte1 & TRAILER_MORE_CHUNKS) !== 0;
  const length = (trailerByte1 & TRAILER_LENGTH) | (twoBytes ? pageArray[trailerPointer - 1] << 4 : 0);
  const trailerStart = trailerPointer - (twoBytes ? 1 : 0);
  const chunkEnd = trailerStart - (moreChunks ? 2 : 0);
  const nextTrailerPointer = moreChunks ? (pageArray[chunkEnd] << 8) | pageArray[chunkEnd + 1] : undefined;
  return [length, chunkEnd, nextTrailerPointer];
}

function readUsePrefix(pageArray: Uint8Array, trailerPointer: number): boolean {
  return (pageArray[trailerPointer] & TRAILER_USE_PREFIX) !== 0;
}

function readChunks(pageArray: Uint8Array, trailerPointer: number): Uint8Array {
  let currentTrailerPointer = trailerPointer;
  let result: Uint8Array | undefined = undefined;
  while (true) {
    if (readUsePrefix(pageArray, currentTrailerPointer)) {
      throw new Error("readChunks does not support prefixes");
    }
    const [length, chunkEnd, nextTrailerPointer] = readLengthChunkEndAndNextTrailerPointer(
      pageArray,
      currentTrailerPointer
    );
    const chunk = pageArray.slice(chunkEnd - length, chunkEnd);
    result = result ? concat(result, chunk) : chunk;
    if (nextTrailerPointer === undefined) {
      return result;
    }
    currentTrailerPointer = nextTrailerPointer;
  }
}

function readEntry(pageArray: Uint8Array, entryCount: number, index: number, entryCache: Uint8Array[]): Uint8Array {
  if (index < 0 || index >= entryCount) {
    throw new Error("invalid index: " + index);
  }
  const cachedResult = entryCache[index];
  if (cachedResult) {
    return cachedResult;
  }
  const trailerPointer = readUint16(pageArray, ENTRIES + index * 2);
  let result: Uint8Array;
  if (trailerPointer === 0) {
    // special case for empty array
    result = new Uint8Array(0);
  } else {
    if (readUsePrefix(pageArray, trailerPointer)) {
      const prefixAfter = (pageArray[trailerPointer] & TRAILER_USE_PREFIX_AFTER) !== 0;
      const [length, _, nextTrailerPointer] = readLengthChunkEndAndNextTrailerPointer(pageArray, trailerPointer);
      const otherEntry = readEntry(pageArray, entryCount, index + (prefixAfter ? 1 : -1), entryCache);
      if (otherEntry.length < length) {
        throw new Error("otherEntry is too short for prefix length: " + otherEntry.length + ", " + length);
      }
      const prefixChunk = otherEntry.slice(0, length);
      if (nextTrailerPointer !== undefined) {
        result = concat(prefixChunk, readChunks(pageArray, nextTrailerPointer));
      } else {
        result = prefixChunk;
      }
    } else {
      result = readChunks(pageArray, trailerPointer);
    }
  }
  entryCache[index] = result;
  return result;
}

export function readAllPageEntries(pageArray: Uint8Array): Uint8Array[] {
  const result: Uint8Array[] = [];
  const entryCount = readEntryCount(pageArray);
  const entryCache: Uint8Array[] = [];
  for (let i = 0; i < entryCount; i++) {
    result.push(readEntry(pageArray, entryCount, i, entryCache));
  }
  return result;
}

function compare(array1: Uint8Array, array2: Uint8Array): -1 | 0 | 1 {
  if (array1 === array2) {
    return 0;
  }
  const length = Math.min(array1.length, array2.length);
  for (let i = 0; i < length; i++) {
    const diff = array1[i] - array2[i];
    if (diff < 0) {
      return -1;
    }
    if (diff > 0) {
      return 1;
    }
  }
  const after1: number | undefined = array1[length];
  const after2: number | undefined = array2[length];
  if (after1 !== after2) {
    // one of them must be undefined
    return after1 === undefined ? -1 : 1;
  }
  return 0;
}

/**
 * @returns an array containing the index where the entry either exists or would be inserted and whether it exists
 */
function findEntryIndex(
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
    const currentIndex = Math.floor((left + right) / 2);
    const currentEntry = readEntry(pageArray, entryCount, currentIndex, entryCache);
    const compareResult = compare(entry, currentEntry);
    if (compareResult === 0) {
      // found the entry
      return [currentIndex, true];
    }
    if (
      left === right ||
      (left === currentIndex && compareResult < 0) ||
      (currentIndex === right && compareResult > 0)
    ) {
      // if entry is smaller, then insert at the current index, otherwise after it
      return [currentIndex + (compareResult > 0 ? 1 : 0), false];
    }
    if (compareResult < 0) {
      right = currentIndex - 1;
    } else {
      left = currentIndex + 1;
    }
  }

  throw new Error("findEntryIndex did not find an index");
}

// export function scan() {}
// export function count() {}

function initIfNecessary(pageArray: Uint8Array): void {
  if (pageArray[0] === 0) {
    pageArray[0] = 1;
    writeUint16(pageArray, FREE_SPACE_POINTER, pageArray.length - 1);
    writeUint16(pageArray, ENTRY_COUNT, 0);
    // this will read as the whole range between the end of the entries array and the end of the page
    pageArray[pageArray.length - 1] = 0;
  }
}

function enoughBytesAvailable(pageArray: Uint8Array, entryCount: number, entryLength: number): boolean {
  let currentTrailerPointer: number | undefined = readUint16(pageArray, FREE_SPACE_POINTER);

  let available = 0;
  while (currentTrailerPointer !== undefined) {
    const [length, chunkEnd, nextTrailerPointer] = readLengthChunkEndAndNextTrailerPointer(
      pageArray,
      currentTrailerPointer
    );
    if (length === 0) {
      if (nextTrailerPointer !== undefined) {
        throw new Error("unexpected nextTrailerPointer");
      }
      // special chunk that goes to the end of the entries array with one additional entry
      const entriesArrayEndPlusOneEntry = ENTRIES + (entryCount + 1) * 2;
      // always assume two byte trailer, so subtract one extra byte and another extra one for the new trailer
      available += chunkEnd - entriesArrayEndPlusOneEntry - 2;
    } else {
      available += length;
    }
    if (available >= entryLength) {
      return true;
    }
    currentTrailerPointer = nextTrailerPointer;
  }

  return false;
}

function writeEntry(pageArray: Uint8Array, entryCount: number, entry: Uint8Array): number {
  // the result will always be the old free space pointer
  const entryTrailerPointer = readUint16(pageArray, FREE_SPACE_POINTER);
  let currentTrailerPointer: number | undefined = entryTrailerPointer;

  let rest = entry;
  while (currentTrailerPointer !== undefined) {
    const [length, chunkEnd, nextTrailerPointer] = readLengthChunkEndAndNextTrailerPointer(
      pageArray,
      currentTrailerPointer
    );
    const restLength = rest.length;
    if (length === 0) {
      if (nextTrailerPointer !== undefined) {
        throw new Error("unexpected nextTrailerPointer");
      }
      // special chunk that goes to the end of the entries array with one additional entry
      const entriesArrayEndPlusOneEntry = ENTRIES + (entryCount + 1) * 2;
      // always assume two byte trailer, so subtract one extra byte and another extra one for the new trailer
      if (restLength > chunkEnd - entriesArrayEndPlusOneEntry - 2) {
        // should not happen
        break;
      }
      const twoBytes = restLength > TRAILER_LENGTH;
      pageArray[currentTrailerPointer] = restLength & TRAILER_LENGTH;
      if (twoBytes) {
        const trailerByte2 = restLength >> 4;
        if (trailerByte2 > 0xff) {
          throw new Error("unexpected length: " + restLength);
        }
        pageArray[currentTrailerPointer - 1] = trailerByte2;
      }
      const realChunkEnd = currentTrailerPointer - (twoBytes ? 1 : 0);
      const realChunkStart = realChunkEnd - restLength;
      pageArray.set(rest, realChunkStart);
      // write the new free space list head
      pageArray[realChunkStart - 1] = 0;
      writeUint16(pageArray, FREE_SPACE_POINTER, realChunkStart - 1);
      return entryTrailerPointer;
    } else {
      // TODO
      throw new Error("not implemented yet");
      currentTrailerPointer = nextTrailerPointer;
    }
  }

  // should not happen enoughBytesAvailable() should be called before this method
  throw new Error("not enough space available");
}

/**
 * @returns whether the insert was successful (i.e. there was sufficient space), if the the entry already existed, then
 *          true is also returned
 */
export function insertPageEntry(pageArray: Uint8Array, entry: Uint8Array): boolean {
  if (entry.length > MAX_ENTRY_LENGTH) {
    throw new Error("entry is too long");
  }
  const entryCount = readEntryCount(pageArray);
  const entryCache: Uint8Array[] = [];
  const [index, exists] = findEntryIndex(pageArray, entryCount, entry, entryCache);
  if (exists) {
    // entry already exists
    return true;
  }
  // we need to insert
  if (entryCount === 0) {
    initIfNecessary(pageArray);
  }
  let trailerPointer: number;
  if (entry.length === 0) {
    // just use 0 as trailer pointer
    trailerPointer = 0;
  } else {
    if (!enoughBytesAvailable(pageArray, entryCount, entry.length)) {
      return false;
    }

    trailerPointer = writeEntry(pageArray, entryCount, entry);
  }
  // shift entries before inserting
  for (let i = entryCount; i > index; i--) {
    const base = ENTRIES + i * 2;
    pageArray[base] = pageArray[base - 2];
    pageArray[base + 1] = pageArray[base - 1];
  }
  // write the trailerPointer
  writeUint16(pageArray, ENTRIES + index * 2, trailerPointer);
  // and increase the count
  writeUint16(pageArray, ENTRY_COUNT, entryCount + 1);
  return true;
}

export function removePageEntry(pageArray: Uint8Array, entry: Uint8Array): boolean {
  throw new Error("TODO");
}
