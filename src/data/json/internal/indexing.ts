import { findFirstBtreeEntryWithPrefix, insertBtreeEntry, removeBtreeEntry } from "../../btree/btree";
import { PageProviderForWrite } from "../../btree/pageProvider";
import { assert } from "../../misc/assert";
import { Uint8ArraySet } from "../../uint8-array/Uint8ArraySet";
import { readBitset32, writeBitset32 } from "../../uint8-array/bitset32";
import { getCompressedUint32ByteLength, writeCompressedUint32 } from "../../uint8-array/compressedUint32";
import { writeOrderPreservingFloat39 } from "../../uint8-array/orderPreservingFloat39";
import {
  JSON_FALSE,
  JSON_NULL,
  JSON_NUMBER,
  JSON_STRING,
  JSON_TRUE,
  NumericJsonEvent,
  getJsonPathAndTypeNumber,
} from "../jsonEvents";

// TODO: find the actually required size for scratchArray
const scratchArray = new Uint8Array(100);

// TODO: add documentation/reasons
function getNumberIndexValue(value: number): Uint8Array {
  writeOrderPreservingFloat39(scratchArray, 0, value);
  return scratchArray.subarray(0, 5);
}

// TODO: what is the best value for this?
const MAX_STRING_INDEX_VALUE_BYTES = 8;

const textEncoder = new TextEncoder();

// TODO: add documentation/reasons
function getStringIndexValue(value: string): Uint8Array {
  const toEncode =
    value.length > MAX_STRING_INDEX_VALUE_BYTES ? value.substring(0, MAX_STRING_INDEX_VALUE_BYTES) : value;
  const bytesWritten = textEncoder.encodeInto(toEncode, scratchArray).written;
  assert(bytesWritten && bytesWritten >= toEncode.length, "unexpected bytesWritten");
  const valueBytesLength = Math.min(bytesWritten, MAX_STRING_INDEX_VALUE_BYTES);

  // replace all 0 bytes, since we use that as a "terminator"
  for (let i = 0; i < valueBytesLength; i++) {
    if (scratchArray[i] === 0) {
      scratchArray[i] = 1;
    }
  }

  // set the "terminator"
  scratchArray[valueBytesLength] = 0;
  return scratchArray.subarray(0, valueBytesLength + 1);
}

const SHARED_DUMMY_ID_INDEX_ENTRY = Uint8Array.from([0]);

export function buildIndexEntries(jsonEvents: NumericJsonEvent[]): Uint8ArraySet {
  const result = new Uint8ArraySet();

  // add the "dummy" entry for the id
  result.add(SHARED_DUMMY_ID_INDEX_ENTRY);

  for (const { pathNumber, type, value } of jsonEvents) {
    let valueBytes: Uint8Array | undefined = undefined;
    switch (type) {
      case JSON_NULL:
      case JSON_TRUE:
      case JSON_FALSE:
        // no valueBytes
        break;
      case JSON_NUMBER:
        valueBytes = getNumberIndexValue(value as number);
        break;
      case JSON_STRING:
        valueBytes = getStringIndexValue(value as string);
        break;

      default:
        // skip events of other types, they are not relevant for indexing
        continue;
    }
    const pathAndTypeNumber = getJsonPathAndTypeNumber(pathNumber, type);
    const pathAndTypeNumberLength = getCompressedUint32ByteLength(pathAndTypeNumber);
    const indexEntry = new Uint8Array(pathAndTypeNumberLength + (valueBytes?.length ?? 0));
    writeCompressedUint32(indexEntry, 0, pathAndTypeNumber);
    if (valueBytes) {
      indexEntry.set(valueBytes, pathAndTypeNumberLength);
    }
    // pathAndTypeNumber 0 is reserved for the dummy id index entries
    assert(indexEntry[0] !== 0, "unexpected pathAndTypeNumber");
    result.add(indexEntry);
  }

  return result;
}

// number of bits that are truncated from the id, because they are handled via the bitset
// TODO: add documentation/reasons
const TRUNCATED_BITS = 5;
const TRUNCATED_MASK = (1 << TRUNCATED_BITS) - 1;

/**
 * The result is always a sub array of scratchArray (starting at 0), so those bytes in scratchArray can be reused...
 */
function buildEntryPrefix(jsonId: number, indexEntry: Uint8Array): Uint8Array {
  scratchArray.set(indexEntry);
  const idLength = writeCompressedUint32(scratchArray, indexEntry.length, jsonId >>> TRUNCATED_BITS);
  const length = indexEntry.length + idLength;
  assert(length <= scratchArray.length);
  return scratchArray.subarray(0, length);
}

function notFalse<T>(value: T | false): T {
  assert(value !== false);
  return value;
}

function updateIndexEntry(
  jsonId: number,
  indexEntry: Uint8Array,
  isAdd: boolean,
  indexRootPageNumber: number,
  pageProvider: PageProviderForWrite
): void {
  const prefix = buildEntryPrefix(jsonId, indexEntry);
  const existingEntry = notFalse(findFirstBtreeEntryWithPrefix(pageProvider.getPage, indexRootPageNumber, prefix));
  let existingBits = 0;
  if (existingEntry) {
    const readResult = readBitset32(existingEntry, prefix.length);
    // check that the entry is "valid"/completely consumed
    assert(prefix.length + readResult.length === existingEntry.length);
    existingBits = readResult.bits;
  }

  let newBits = existingBits;
  const bit = 1 << (jsonId & TRUNCATED_MASK);
  if (isAdd) {
    newBits |= bit;
  } else {
    newBits &= ~bit;
  }
  newBits = newBits >>> 0;

  if (newBits !== existingBits) {
    if (existingEntry) {
      removeBtreeEntry(pageProvider, indexRootPageNumber, existingEntry);
    }
    if (newBits !== 0) {
      // prefix is already in scratchArray, see buildEntryPrefix()
      const bitsLength = writeBitset32(scratchArray, prefix.length, newBits);
      const newEntry = scratchArray.subarray(0, prefix.length + bitsLength);
      insertBtreeEntry(pageProvider, indexRootPageNumber, newEntry);
    }
  }
}

function addIndexEntry(
  jsonId: number,
  indexEntry: Uint8Array,
  indexRootPageNumber: number,
  pageProvider: PageProviderForWrite
): void {
  updateIndexEntry(jsonId, indexEntry, true, indexRootPageNumber, pageProvider);
}

function removeIndexEntry(
  jsonId: number,
  indexEntry: Uint8Array,
  indexRootPageNumber: number,
  pageProvider: PageProviderForWrite
): void {
  updateIndexEntry(jsonId, indexEntry, false, indexRootPageNumber, pageProvider);
}

/**
 * Only performs the necessary index updates. Assumes that all entries in indexEntriesBefore already exist (without
 * checking it).
 */
export function updateIndexForJson(
  jsonId: number,
  indexEntriesBefore: Uint8ArraySet | undefined,
  indexEntriesAfter: Uint8ArraySet | undefined,
  indexRootPageNumber: number,
  pageProvider: PageProviderForWrite
): void {
  if (!indexEntriesAfter) {
    // just remove all index entries
    indexEntriesBefore?.forEach((indexEntry) => {
      removeIndexEntry(jsonId, indexEntry, indexRootPageNumber, pageProvider);
    });
  } else {
    // copy indexEntriesBefore (we do not want to modify it)
    const unhandledEntriesBefore = indexEntriesBefore?.copy();
    indexEntriesAfter.forEach((indexEntry, hash) => {
      const entryAlreadyExists = unhandledEntriesBefore?.delete(indexEntry, hash);
      if (!entryAlreadyExists) {
        addIndexEntry(jsonId, indexEntry, indexRootPageNumber, pageProvider);
      }
    });
    // remove the remaining unhandled entries
    unhandledEntriesBefore?.forEach((indexEntry) => {
      removeIndexEntry(jsonId, indexEntry, indexRootPageNumber, pageProvider);
    });
  }
}
