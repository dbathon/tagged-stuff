import { expect, test } from "vitest";
import { readOrderPreservingFloat32, writeOrderPreservingFloat32 } from "./orderPreservingFloat32";

const FRACTION_BITS = 23;
const EXPONENT_BITS = 8;

const FRACTION_MASK = (1 << FRACTION_BITS) - 1;
const EXPONENT_MASK = (1 << EXPONENT_BITS) - 1;

const scratchArray = new Uint8Array(6);
const scratchDataView = new DataView(scratchArray.buffer);

function toFloat32(exponent: number, fraction: number, negative: boolean) {
  const floatBits =
    (((exponent & EXPONENT_MASK) << FRACTION_BITS) | (fraction & FRACTION_MASK)) + (negative ? 0x80000000 : 0);
  scratchDataView.setUint32(0, floatBits);
  return scratchDataView.getFloat32(0);
}

test("write/readOrderPreservingFloat32", () => {
  const fractions = [
    0,
    (FRACTION_MASK / 4) >>> 0,
    (FRACTION_MASK / 2) >>> 0,
    ((FRACTION_MASK * 3) / 4) >>> 0,
    FRACTION_MASK,
  ];

  const interestingFloats: number[] = [];
  for (let i = 0; i <= EXPONENT_MASK; i++) {
    if (i < 4 || i > 251 || i === 127 || i % 32 === 0) {
      for (const fraction of fractions) {
        interestingFloats.push(toFloat32(i, fraction, true), toFloat32(i, fraction, false));
      }
    }
  }

  const sortedNonNanFloats = interestingFloats.filter((float) => float === float).sort((a, b) => a - b);

  function testRoundtrip(float: number) {
    writeOrderPreservingFloat32(scratchArray, 1, float);
    expect(readOrderPreservingFloat32(scratchArray, 1)).toBe(float);
  }

  let previousUint32: number | undefined = undefined;
  for (const float of sortedNonNanFloats) {
    testRoundtrip(float);

    writeOrderPreservingFloat32(scratchArray, 2, float);
    const uint32 = scratchDataView.getUint32(2);
    if (previousUint32 !== undefined) {
      expect(uint32).toBeGreaterThan(previousUint32);
    }
    previousUint32 = uint32;
  }

  testRoundtrip(NaN);
});
