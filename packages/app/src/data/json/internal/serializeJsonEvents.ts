import {
  assert,
  getCompressedFloat64ByteLength,
  readCompressedFloat64,
  writeCompressedFloat64,
  readCompressedUint32,
  writeCompressedUint32,
  getCompressedUint32ByteLength,
} from "shared-util";
import {
  type BaseJsonEvent,
  JSON_NUMBER,
  JSON_STRING,
  type JsonEventType,
  getJsonPathAndTypeNumber,
  getJsonPathNumberAndTypeFromPathAndTypeNumber,
} from "../jsonEvents";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

// limit the max size of events to allow encoding some extra flags
const MAX_EVENTS_SIZE = (1 << 28) - 1;

/**
 * Serializes JsonEvents including the values into an Uint8Array.
 *
 * The "encoding" tries to minimize the number of required bytes, but still includes information to allow some level of
 * "random access" without parsing everything.
 *
 * @param jsonEvents
 * @returns the resulting array
 */
export function serializeJsonEvents<E extends BaseJsonEvent>(
  jsonEvents: E[],
  getPathNumber: (jsonEvent: E) => number | undefined,
): Uint8Array {
  const pathAndTypeNumbers: number[] = [];
  let eventsSize = 0;

  const numbers: number[] = [];
  let numbersSize = 0;

  const encodedStrings: Uint8Array[] = [];
  let stringLengthsSize = 0;
  let stringsSize = 0;

  // first loop over all events to collect the values and determine the required sizes
  for (const jsonEvent of jsonEvents) {
    const pathAndTypeNumber = getJsonPathAndTypeNumber(getPathNumber(jsonEvent), jsonEvent.type);
    pathAndTypeNumbers.push(pathAndTypeNumber);
    eventsSize += getCompressedUint32ByteLength(pathAndTypeNumber);

    const value = jsonEvent.value;
    switch (jsonEvent.type) {
      case JSON_NUMBER:
        assert(typeof value === "number");
        numbers.push(value);
        numbersSize += getCompressedFloat64ByteLength(value);
        break;
      case JSON_STRING:
        assert(typeof value === "string");
        const encodedString = textEncoder.encode(value);
        encodedStrings.push(encodedString);
        stringLengthsSize += getCompressedUint32ByteLength(encodedString.length);
        stringsSize += encodedString.length;
        break;
      default:
        assert(value === undefined);
    }
  }

  assert(eventsSize <= MAX_EVENTS_SIZE, "too many events");

  // then write the events and values
  // encode whether numbers or strings exist in the first number
  const eventsSizeAndFlags = (eventsSize << 2) | (numbers.length ? 0b10 : 0) | (encodedStrings.length ? 0b01 : 0);
  const headerSize =
    getCompressedUint32ByteLength(eventsSizeAndFlags) +
    (numbers.length ? getCompressedUint32ByteLength(numbersSize) : 0) +
    (encodedStrings.length
      ? getCompressedUint32ByteLength(stringLengthsSize) + getCompressedUint32ByteLength(stringsSize)
      : 0);
  const totalSize = headerSize + eventsSize + numbersSize + stringLengthsSize + stringsSize;

  const result = new Uint8Array(totalSize);
  let offset = writeCompressedUint32(result, 0, eventsSizeAndFlags);
  if (numbers.length) {
    offset += writeCompressedUint32(result, offset, numbersSize);
  }
  if (encodedStrings.length) {
    offset += writeCompressedUint32(result, offset, stringLengthsSize);
    offset += writeCompressedUint32(result, offset, stringsSize);
  }
  assert(offset === headerSize);

  for (const pathAndTypeNumber of pathAndTypeNumbers) {
    offset += writeCompressedUint32(result, offset, pathAndTypeNumber);
  }
  assert(offset === headerSize + eventsSize);

  for (const number of numbers) {
    offset += writeCompressedFloat64(result, offset, number);
  }
  assert(offset === headerSize + eventsSize + numbersSize);

  for (const encodedString of encodedStrings) {
    offset += writeCompressedUint32(result, offset, encodedString.length);
  }
  assert(offset === headerSize + eventsSize + numbersSize + stringLengthsSize);

  for (const encodedString of encodedStrings) {
    result.set(encodedString, offset);
    offset += encodedString.length;
  }
  assert(offset === totalSize);

  return result;
}

function readHeader(array: Uint8Array): {
  headerSize: number;
  eventsSize: number;
  numbersSize: number;
  stringLengthsSize: number;
  stringsSize: number;
  totalSize: number;
} {
  const { uint32: eventsSizeAndFlags, length: eventsSizeAndFlagsLength } = readCompressedUint32(array, 0);
  const eventsSize = eventsSizeAndFlags >>> 2;
  let offset = eventsSizeAndFlagsLength;

  let numbersSize = 0;
  if (eventsSizeAndFlags & 0b10) {
    const readResult = readCompressedUint32(array, offset);
    numbersSize = readResult.uint32;
    offset += readResult.length;
  }

  let stringLengthsSize = 0;
  let stringsSize = 0;
  if (eventsSizeAndFlags & 0b01) {
    const readResult1 = readCompressedUint32(array, offset);
    stringLengthsSize = readResult1.uint32;
    offset += readResult1.length;

    const readResult2 = readCompressedUint32(array, offset);
    stringsSize = readResult2.uint32;
    offset += readResult2.length;
  }

  const headerSize = offset;
  const totalSize = headerSize + eventsSize + numbersSize + stringLengthsSize + stringsSize;

  return { headerSize, eventsSize, numbersSize, stringLengthsSize, stringsSize, totalSize };
}

// TODO implement variations of this method that can read partial data without deserializing everything...
export function deserializeJsonEvents<E>(
  array: Uint8Array,
  eventFactory: (pathNumber: number | undefined, type: JsonEventType, value: string | number | undefined) => E,
): E[] {
  const { headerSize, eventsSize, numbersSize, stringLengthsSize, totalSize } = readHeader(array);
  assert(array.length === totalSize);

  const eventsEnd = headerSize + eventsSize;

  const numbers: number[] = [];
  const numbersEnd = eventsEnd + numbersSize;
  for (let offset = eventsEnd; offset < numbersEnd; ) {
    const { value, length } = readCompressedFloat64(array, offset);
    numbers.push(value);
    offset += length;
  }

  const strings: string[] = [];
  const stringLengthsEnd = numbersEnd + stringLengthsSize;
  let nextStringOffset = stringLengthsEnd;
  for (let offset = numbersEnd; offset < stringLengthsEnd; ) {
    const { uint32: encodedStringLength, length } = readCompressedUint32(array, offset);
    offset += length;

    strings.push(textDecoder.decode(array.subarray(nextStringOffset, nextStringOffset + encodedStringLength)));
    nextStringOffset += encodedStringLength;
  }
  assert(nextStringOffset === totalSize);

  let numbersIndex = 0;
  let stringsIndex = 0;
  const result: E[] = [];
  for (let offset = headerSize; offset < eventsEnd; ) {
    const { uint32: pathAndTypeNumber, length } = readCompressedUint32(array, offset);
    offset += length;

    const { pathNumber, type } = getJsonPathNumberAndTypeFromPathAndTypeNumber(pathAndTypeNumber);

    let value: string | number | undefined = undefined;
    if (type === JSON_NUMBER) {
      value = numbers[numbersIndex++];
    } else if (type === JSON_STRING) {
      value = strings[stringsIndex++];
    }

    result.push(eventFactory(pathNumber, type, value));
  }

  assert(numbersIndex === numbers.length && stringsIndex === strings.length);

  return result;
}
