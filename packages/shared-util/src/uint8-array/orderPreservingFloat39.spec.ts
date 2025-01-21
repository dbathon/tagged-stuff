import { assert, expect, test } from "vitest";
import { readOrderPreservingFloat39, writeOrderPreservingFloat39 } from "./orderPreservingFloat39";
import { compareUint8Arrays } from "./compareUint8Arrays";

const FRACTION_BITS = 27;
const EXPONENT_BITS = 11;

const FRACTION_MASK = (1 << FRACTION_BITS) - 1;
const EXPONENT_MASK = (1 << EXPONENT_BITS) - 1;

const scratchArray = new Uint8Array(8);
const scratchDataView = new DataView(scratchArray.buffer);

function toFloat39(exponent: number, fraction: number, negative: boolean) {
  const floatBitsPart1 =
    (negative ? 0x80000000 : 0) +
    (((exponent & EXPONENT_MASK) << (31 - EXPONENT_BITS)) | ((fraction & FRACTION_MASK) >>> 7));
  scratchDataView.setUint32(0, floatBitsPart1);
  scratchDataView.setUint32(4, 0);
  scratchArray[4] = (fraction & 0x7f) << 1;
  return scratchDataView.getFloat64(0);
}

test("write/readOrderPreservingFloat39", () => {
  const fractions = [
    0,
    (FRACTION_MASK / 4) >>> 0,
    (FRACTION_MASK / 2) >>> 0,
    ((FRACTION_MASK * 3) / 4) >>> 0,
    FRACTION_MASK,
  ];

  const interestingFloats: number[] = [];
  for (let i = 0; i <= EXPONENT_MASK; i++) {
    if (i < 4 || i > 2042 || i === 1023 || i % 32 === 0) {
      for (const fraction of fractions) {
        interestingFloats.push(toFloat39(i, fraction, true), toFloat39(i, fraction, false));
      }
    }
  }

  const sortedNonNanFloats = interestingFloats.filter((float) => float === float).sort((a, b) => a - b);

  function testRoundtrip(float: number, expectExact = true) {
    writeOrderPreservingFloat39(scratchArray, 3, float);
    const { value, exact } = readOrderPreservingFloat39(scratchArray, 3);
    if (exact) {
      assert(Object.is(value, float));
    } else {
      scratchDataView.setFloat64(0, float);
      // only preserve the first 39 bits
      scratchArray[4] &= 0xfe;
      scratchArray[5] = 0;
      scratchArray[6] = 0;
      scratchArray[7] = 0;
      assert(Object.is(value, scratchDataView.getFloat64(0)));
    }
    assert(exact === expectExact);
  }

  const testArray = new Uint8Array(7);
  let previousArray: Uint8Array | undefined = undefined;
  for (const float of sortedNonNanFloats) {
    testRoundtrip(float);

    writeOrderPreservingFloat39(testArray, 1, float);
    assert(testArray[0] === 0);
    assert(testArray[6] === 0);
    if (previousArray !== undefined) {
      assert(compareUint8Arrays(previousArray, testArray) === -1);
    }
    previousArray = Uint8Array.from(testArray);
  }

  testRoundtrip(NaN);

  const otherTestCases: (number | [number, boolean])[] = [
    0,
    1,
    1.125,
    1.5,
    1.75,
    2,
    3,
    4,
    5,
    100,
    1000000,
    378509238,
    437864058,
    [23.3456834756873465, false],
    [43.387692876e100, false],
    43e9,
    [43e10, false],
    [43e100, false],
  ];
  for (const testCase of otherTestCases) {
    const [number, expectExact] = typeof testCase === "number" ? [testCase, true] : testCase;
    testRoundtrip(number, expectExact);
    testRoundtrip(-number, expectExact);
  }
});
