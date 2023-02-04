import { expect, test } from "vitest";
import { dataViewsEqual } from "./util";

test("dataViewsEqual", () => {
  const base: number[] = [];
  for (let i = 0; i < 142; i++) {
    const view1 = new DataView(Uint8Array.from(base).buffer);
    const view2 = new DataView(Uint8Array.from(base).buffer);

    expect(dataViewsEqual(view1, view1)).toBe(true);
    expect(dataViewsEqual(view2, view2)).toBe(true);
    expect(dataViewsEqual(view1, view2)).toBe(true);

    for (let j = 0; j < i; j++) {
      const old = view1.getUint8(j);
      view1.setUint8(j, old + 1);
      expect(dataViewsEqual(view1, view2)).toBe(false);
      view1.setUint8(j, old);
      expect(dataViewsEqual(view1, view2)).toBe(true);
    }
    base.push(i);
  }
});
