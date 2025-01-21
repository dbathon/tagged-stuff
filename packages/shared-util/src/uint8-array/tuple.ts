import { assert } from "../misc/assert";
import { getCompressedFloat64ByteLength, readCompressedFloat64, writeCompressedFloat64 } from "./compressedFloat64";
import { getCompressedUint32ByteLength, readCompressedUint32, writeCompressedUint32 } from "./compressedUint32";

export type TupleElementTypeName = "number" | "uint32" | "uint32raw" | "string" | "array";

export type TupleTypeDefinition = readonly TupleElementTypeName[];

export type TupleType<T extends TupleTypeDefinition> = T extends readonly []
  ? readonly []
  : T extends readonly [infer TN, ...infer Rest extends TupleTypeDefinition]
    ? readonly [
        TN extends "number" | "uint32" | "uint32raw"
          ? number
          : TN extends "string"
            ? string
            : TN extends "array"
              ? Uint8Array
              : never,
        ...TupleType<Rest>,
      ]
    : never;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function toLengthsOrArrays<T extends TupleTypeDefinition>(tupleType: T, values: TupleType<T>): (number | Uint8Array)[] {
  const result: (number | Uint8Array)[] = [];
  const length = tupleType.length;
  for (let i = 0; i < length; i++) {
    const typeName = tupleType[i];
    switch (typeName) {
      case "number":
        result.push(getCompressedFloat64ByteLength(values[i] as number));
        break;
      case "uint32":
        result.push(getCompressedUint32ByteLength(values[i] as number));
        break;
      case "uint32raw":
        result.push(4);
        break;
      case "string":
        const encodedString = textEncoder.encode(values[i] as string);
        result.push(getCompressedUint32ByteLength(encodedString.length), encodedString);
        break;
      case "array":
        const bytes = values[i] as Uint8Array;
        result.push(getCompressedUint32ByteLength(bytes.length), bytes);
        break;
    }
  }
  return result;
}

function getLengthSum(lengthsOrArrays: (number | Uint8Array)[]) {
  let result = 0;
  for (const lengthOrArray of lengthsOrArrays) {
    result += typeof lengthOrArray === "number" ? lengthOrArray : lengthOrArray.length;
  }
  return result;
}

/**
 * @param tupleType
 * @param values
 * @returns the number of bytes that will be needed to serialize the give values
 */
export function getTupleByteLength<T extends TupleTypeDefinition>(tupleType: T, values: TupleType<T>): number {
  return getLengthSum(toLengthsOrArrays(tupleType, values));
}

const scratchUint8Array = new Uint8Array(4);
const scratchDataView = new DataView(scratchUint8Array.buffer);

function writeTupleInternal<T extends TupleTypeDefinition>(
  array: Uint8Array,
  offset: number,
  tupleType: T,
  values: TupleType<T>,
  lengthsOrArrays: (number | Uint8Array)[],
): void {
  let lengthsOrArraysIndex = 0;

  const length = tupleType.length;
  for (let i = 0; i < length; i++) {
    const typeName = tupleType[i];
    let bytesWritten = 0;
    let extraArray = false;
    switch (typeName) {
      case "number":
        bytesWritten = writeCompressedFloat64(array, offset, values[i] as number);
        offset += bytesWritten;
        break;
      case "uint32":
        bytesWritten = writeCompressedUint32(array, offset, values[i] as number);
        offset += bytesWritten;
        break;
      case "uint32raw":
        scratchDataView.setUint32(0, values[i] as number);
        array.set(scratchUint8Array, offset);
        bytesWritten = 4;
        offset += bytesWritten;
        break;
      case "string":
      case "array":
        extraArray = true;
        const uint8Array = lengthsOrArrays[lengthsOrArraysIndex + 1];
        assert(uint8Array instanceof Uint8Array);
        bytesWritten = writeCompressedUint32(array, offset, uint8Array.length);
        offset += bytesWritten;
        array.set(uint8Array, offset);
        offset += uint8Array.length;
        break;
    }
    assert(bytesWritten && bytesWritten === lengthsOrArrays[lengthsOrArraysIndex]);
    lengthsOrArraysIndex++;
    if (extraArray) {
      assert(lengthsOrArrays[lengthsOrArraysIndex] instanceof Uint8Array);
      lengthsOrArraysIndex++;
    }
  }
  assert(lengthsOrArraysIndex === lengthsOrArrays.length);
}

/**
 * @param array
 * @param offset
 * @param tupleType
 * @param values
 * @returns the number of bytes written
 */
export function writeTuple<T extends TupleTypeDefinition>(
  array: Uint8Array,
  offset: number,
  tupleType: T,
  values: TupleType<T>,
): number {
  const lengthsOrArrays = toLengthsOrArrays(tupleType, values);
  const length = getLengthSum(lengthsOrArrays);

  if (offset < 0 || offset + length > array.length) {
    throw new RangeError("offset is out of bounds");
  }

  writeTupleInternal(array, offset, tupleType, values, lengthsOrArrays);

  return length;
}

/**
 * @param tupleType
 * @param values
 * @returns a new Uint8Array with the serialized tuple values
 */
export function tupleToUint8Array<T extends TupleTypeDefinition>(tupleType: T, values: TupleType<T>): Uint8Array {
  const lengthsOrArrays = toLengthsOrArrays(tupleType, values);
  const length = getLengthSum(lengthsOrArrays);
  const result = new Uint8Array(length);

  writeTupleInternal(result, 0, tupleType, values, lengthsOrArrays);

  return result;
}

/**
 * Reads a tuple written by writeTuple() or tupleToUint8Array().
 *
 * @param array
 * @param tupleType
 * @param offset
 * @returns the read values and the number of bytes consumed (length)
 */
export function readTuple<T extends TupleTypeDefinition>(
  array: Uint8Array,
  tupleType: T,
  offset: number = 0,
): { values: TupleType<T>; length: number } {
  if (!tupleType.length) {
    return { values: [] as TupleType<T>, length: 0 };
  }
  if (offset < 0 || offset >= array.length) {
    throw new RangeError("offset is out of bounds");
  }

  let index = offset;
  const result: (string | number | Uint8Array)[] = [];
  const length = tupleType.length;
  for (let i = 0; i < length; i++) {
    const typeName = tupleType[i];
    switch (typeName) {
      case "number":
        {
          const readResult = readCompressedFloat64(array, index);
          result.push(readResult.value);
          index += readResult.length;
        }
        break;
      case "uint32":
        {
          const readResult = readCompressedUint32(array, index);
          result.push(readResult.uint32);
          index += readResult.length;
        }
        break;
      case "uint32raw":
        {
          result.push(new DataView(array.buffer, array.byteOffset + index, 4).getUint32(0));
          index += 4;
        }
        break;
      case "string":
      case "array":
        {
          const readResult = readCompressedUint32(array, index);
          const bytesLength = readResult.uint32;
          index += readResult.length;
          const subArray = array.subarray(index, index + bytesLength);
          result.push(typeName === "string" ? textDecoder.decode(subArray) : subArray);
          index += bytesLength;
        }
        break;
    }
    if (index > array.length) {
      throw new Error("array truncated");
    }
  }

  return { values: result as TupleType<T>, length: index - offset };
}
