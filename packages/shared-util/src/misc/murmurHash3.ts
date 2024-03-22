/**
 * This file contains an implementation of the 32 bit version of MurmurHash3, see
 * https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
 *
 * The implementation is extracted and adapted from http://github.com/karanlyons/murmurHash3.js, which is
 * copyright (c) 2012-2020 Karan Lyons. Freely distributable under the MIT license.
 */

function mul32(m: number, n: number): number {
  return (m & 0xffff) * n + ((((m >>> 16) * n) & 0xffff) << 16);
}

function rol32(n: number, r: number): number {
  return (n << r) | (n >>> (32 - r));
}

const c1 = 0xcc9e2d51;
const c2 = 0x1b873593;

export function murmurHash3_x86_32(input: Uint8Array, seed: number = 0): number {
  let h1 = seed;

  const length = input.byteLength;
  const remainder = length % 4;
  let i = 0;

  if (length >= 4) {
    const dataView = new DataView(input.buffer, input.byteOffset);
    const blockBytes = length - remainder;

    for (; i < blockBytes; i += 4) {
      let k1 = dataView.getUint32(i, true);
      k1 = mul32(k1, c1);
      k1 = rol32(k1, 15);
      k1 = mul32(k1, c2);

      h1 ^= k1;
      h1 = rol32(h1, 13);
      h1 = mul32(h1, 5) + 0xe6546b64;
    }
  }

  // finalize
  let k1 = 0;
  switch (remainder) {
    case 3:
      k1 ^= input[i + 2] << 16;
    case 2:
      k1 ^= input[i + 1] << 8;
    case 1:
      k1 ^= input[i];
      k1 = mul32(k1, c1);
      k1 = rol32(k1, 15);
      k1 = mul32(k1, c2);
      h1 ^= k1;
  }

  h1 ^= length & 0xffffffff;

  // finalization mix - force all bits of a hash block to avalanche
  h1 ^= h1 >>> 16;
  h1 = mul32(h1, 0x85ebca6b);
  h1 ^= h1 >>> 13;
  h1 = mul32(h1, 0xc2b2ae35);
  h1 ^= h1 >>> 16;

  return h1 >>> 0;
}
