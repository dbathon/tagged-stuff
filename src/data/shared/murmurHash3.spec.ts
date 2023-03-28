import { expect, test } from "vitest";
import { murmurHash3_x86_32 } from "./murmurHash3";

function testMurmurHash3_x86_32(bytes: number[], expectedHashes: number[]) {
  expectedHashes.forEach((expectedHash, seed) =>
    expect(murmurHash3_x86_32(Uint8Array.from(bytes), seed)).toBe(expectedHash)
  );
}

test("murmurHash3_x86_32", () => {
  // some examples generated with Guava's implementation
  testMurmurHash3_x86_32([0], [1364076727, 0, 2247144487, 821347078, 3423425485, 614249093, 415870660, 1558924552]);
  testMurmurHash3_x86_32(
    [1],
    [3831157163, 693974893, 1803479562, 2511813514, 2138485752, 3657809203, 735125588, 4060411928]
  );
  testMurmurHash3_x86_32(
    [2],
    [1814548639, 4060016154, 280380716, 65183749, 1892868337, 3544434325, 1302829843, 2714465411]
  );
  testMurmurHash3_x86_32(
    [0, 1],
    [1893835456, 2464133615, 892701619, 3244670278, 3892021524, 1190366234, 2290194012, 2634944882]
  );
  testMurmurHash3_x86_32(
    [0, 1, 2],
    [1372901591, 1607262186, 3120249721, 1612312150, 2081197868, 3779344352, 2007935557, 1769403848]
  );
  testMurmurHash3_x86_32(
    [0, 1, 2, 3],
    [4106284089, 1239038639, 2457288289, 2504878183, 569130870, 12402120, 2887626658, 1217798584]
  );
  testMurmurHash3_x86_32(
    [0, 1, 2, 3, 4],
    [3433356491, 225793675, 960192911, 3771078742, 234790329, 3170526822, 1518175819, 1200999764]
  );
  testMurmurHash3_x86_32(
    [0, 1, 2, 3, 4, 5],
    [2417804034, 1428323894, 30925542, 2573228019, 447996974, 111016751, 2145012386, 2861890291]
  );
  testMurmurHash3_x86_32(
    [0, 1, 2, 3, 4, 5, 6],
    [2373863700, 3089766270, 1722552325, 3788880866, 2749865405, 3440179074, 1371577136, 3107475203]
  );
  testMurmurHash3_x86_32(
    [0, 1, 2, 3, 4, 5, 6, 7],
    [3512850035, 4261129010, 3374804381, 446238306, 2225541670, 2828508282, 3642008492, 3473660818]
  );
  testMurmurHash3_x86_32(
    [0, 1, 2, 3, 4, 5, 6, 7, 8],
    [3890379344, 268665067, 626698273, 2603174716, 1632709196, 3665597734, 3726278135, 2660354422]
  );
  testMurmurHash3_x86_32(
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [3585761019, 2550342716, 3476713792, 3957711136, 2501925887, 3339132860, 1171807787, 205848919]
  );
  testMurmurHash3_x86_32(
    [9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
    [2987317286, 915496538, 293538465, 3695092698, 1224957263, 49385876, 1756224188, 2918171098]
  );
  testMurmurHash3_x86_32(
    [32, 64, 128, 192, 253, 254, 255],
    [3690247084, 3565786610, 1123180697, 2750706209, 1799560537, 383782518, 1586498737, 1687654323]
  );
});
