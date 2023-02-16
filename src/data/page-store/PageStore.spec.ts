import { describe, expect, test } from "vitest";
import { InMemoryPageStoreBackend } from "./InMemoryPageStoreBackend";
import { dataViewsEqual } from "./internal/util";
import { PageStore } from "./PageStore";

const PAGE_SIZE = 8192;

function xorShift32(x: number): number {
  /* Algorithm "xor" from p. 4 of Marsaglia, "Xorshift RNGs" */
  x ^= x << 13;
  x ^= x >> 17;
  x ^= x << 5;
  return x;
}

function fillRandom(dataView: DataView, writes: number, seed: number) {
  let x = seed === 0 ? 1 : seed;
  const maxOffset = dataView.byteLength - 4;
  for (let i = 0; i < writes; i++) {
    x = xorShift32(x);
    const offset = Math.abs(x) % (maxOffset + 1);
    dataView.setInt32(offset, x);
  }
}

function expectEqualsFillRandom(dataView: DataView, writes: number, seed: number) {
  const expected = new DataView(new ArrayBuffer(dataView.byteLength));
  fillRandom(expected, writes, seed);
  expect(dataViewsEqual(expected, dataView)).toBe(true);
}

describe("PageStore", () => {
  describe("read from empty store", () => {
    test("should return pages with zero bytes", async () => {
      const backend = new InMemoryPageStoreBackend(PAGE_SIZE);
      const store = new PageStore(backend);
      expect(store.loading).toBe(false);
      const page0 = store.getPage(0);
      const page2 = store.getPage(2);
      expect(page0.value).toBe(undefined);
      expect(page2.value).toBe(undefined);

      expect(store.loading).toBe(true);
      await store.loadingFinished();
      expect(store.loading).toBe(false);

      expect(page0.value!.array[0]).toBe(0);
      expect(page0.value!.array[1]).toBe(0);
      expect(page0.value!.array[5]).toBe(0);
      expect(page0.value!.array[store.pageSize - 1]).toBe(0);
      expect(page0.value!.array[store.pageSize]).toBe(undefined);

      expect(page2.value!.array[5]).toBe(0);
    });
  });

  describe("constructor page size validations", () => {
    test("works", () => {
      [2, (1 << 12) - 1, (1 << 16) + 1, 1 << 17].forEach((pageSize) => {
        expect(() => new PageStore(new InMemoryPageStoreBackend(pageSize))).toThrow();
      });
      [1 << 12, 1 << 16].forEach((pageSize) => {
        expect(() => new PageStore(new InMemoryPageStoreBackend(pageSize))).not.toThrow();
      });
    });
  });

  describe("transaction", () => {
    test("should work if there are no conflicts", async () => {
      const backend = new InMemoryPageStoreBackend(PAGE_SIZE);
      const store = new PageStore(backend);
      const page0 = store.getPage(0);
      await store.loadingFinished();

      const store2 = new PageStore(backend);
      const store2Page0 = store2.getPage(0);
      await store2.loadingFinished();

      expect(page0.value?.array[0]).toBe(0);
      expect(page0.value?.array[1]).toBe(0);
      expect(store2Page0.value?.array[0]).toBe(0);
      expect(store2Page0.value?.array[1]).toBe(0);

      const result = await store.runTransaction(() => {
        store.getPageDataForUpdate(0).array[0] = 42;

        expect(page0.value?.array[0]).toBe(42);
        expect(page0.value?.array[1]).toBe(0);
        expect(store2Page0.value?.array[0]).toBe(0);
        expect(store2Page0.value?.array[1]).toBe(0);
      });

      expect(result.committed).toBe(true);

      expect(page0.value?.array[0]).toBe(42);
      expect(page0.value?.array[1]).toBe(0);
      expect(store2Page0.value?.array[0]).toBe(0);
      expect(store2Page0.value?.array[1]).toBe(0);

      store2.refresh();
      await store2.loadingFinished();
      expect(store2Page0.value?.array[0]).toBe(42);
      expect(store2Page0.value?.array[1]).toBe(0);
    });

    test("should fail if there are conflicts", async () => {
      const backend = new InMemoryPageStoreBackend(PAGE_SIZE);
      const store = new PageStore(backend);
      const page0 = store.getPage(0);
      const page1 = store.getPage(1);
      await store.loadingFinished();

      const store2 = new PageStore(backend);
      const store2Page0 = store2.getPage(0);
      await store2.loadingFinished();

      expect(dataViewsEqual(page0.value!.dataView, new DataView(new ArrayBuffer(store.pageSize)))).toBe(true);
      expect(dataViewsEqual(page1.value!.dataView, new DataView(new ArrayBuffer(store.pageSize)))).toBe(true);

      {
        const result = await store.runTransaction(() => {
          store.getPageDataForUpdate(0).array[0] = 42;

          expect(page0.value?.array[0]).toBe(42);
        });
        expect(result.committed).toBe(true);
      }
      expect(page0.value?.array[0]).toBe(42);

      // commit without retry in store2 should fail
      expect(store2Page0.value?.array[0]).toBe(0);
      {
        const result = await store2.runTransaction(() => {
          store2.getPageDataForUpdate(0).array[0] = 43;

          expect(store2Page0.value?.array[0]).toBe(43);
        }, 0);
        expect(result.committed).toBe(false);
      }
      expect(store2Page0.value?.array[0]).toBe(42);

      // 2nd try should work
      {
        const result = await store2.runTransaction(() => {
          store2.getPageDataForUpdate(0).array[0] = 43;

          expect(store2Page0.value?.array[0]).toBe(43);
        }, 0);
        expect(result.committed).toBe(true);
      }
      expect(store2Page0.value?.array[0]).toBe(43);

      // and with retry should also work (back in the first store)
      expect(page0.value?.array[0]).toBe(42);
      const seenPrevValues: number[] = [];
      {
        const result = await store.runTransaction(() => {
          const pageData = store.getPageDataForUpdate(0);
          seenPrevValues.push(pageData.array[0]);
          pageData.array[0] = 44;

          expect(page0.value?.array[0]).toBe(44);
        });
        expect(result.committed).toBe(true);
      }
      expect(page0.value?.array[0]).toBe(44);
      expect(seenPrevValues).toStrictEqual([42, 43]);
    });

    test("should work with more data, but still only index page", async () => {
      const backend = new InMemoryPageStoreBackend(PAGE_SIZE);
      const store = new PageStore(backend);

      const pageCount = 10;
      const writeCount = 20;

      const result = await store.runTransaction(() => {
        for (let i = 0; i < pageCount; i++) {
          fillRandom(store.getPageDataForUpdate(i).dataView, writeCount, i + 1);
        }
      });
      expect(result.committed).toBe(true);

      for (let i = 0; i < pageCount; i++) {
        expectEqualsFillRandom(store.getPage(i).value?.dataView!, writeCount, i + 1);
      }

      const store2 = new PageStore(backend);
      for (let i = 0; i < pageCount; i++) {
        const page = store2.getPage(i);
        if (i === 0) {
          expect(store2.loading).toBe(true);
          await store2.loadingFinished();
        } else {
          // no more loading needed
          expect(store2.loading).toBe(false);
        }
        expectEqualsFillRandom(page.value?.dataView!, writeCount, i + 1);
      }

      // load pages after the first page group, those should also not require more loading
      store2.getPage(40);
      store2.getPage(100);
      expect(store2.loading).toBe(false);

      // everything is stored in the index page
      expect(backend.pages.size).toBe(1);
      expect([...backend.pages.keys()]).toEqual([-1]);
    });

    test("should work with even more data, by moving the diffs to to the page group pages", async () => {
      const backend = new InMemoryPageStoreBackend(PAGE_SIZE);
      const store = new PageStore(backend);

      const pageCount = 100;
      const writeCount = 20;

      const result = await store.runTransaction(() => {
        for (let i = 0; i < pageCount; i++) {
          fillRandom(store.getPageDataForUpdate(i).dataView, writeCount, i + 1);
        }
      });
      expect(result.committed).toBe(true);

      for (let i = 0; i < pageCount; i++) {
        expectEqualsFillRandom(store.getPage(i).value?.dataView!, writeCount, i + 1);
      }

      const store2 = new PageStore(backend);
      for (let i = 0; i < pageCount; i++) {
        const page = store2.getPage(i);
        await store2.loadingFinished();
        expectEqualsFillRandom(page.value?.dataView!, writeCount, i + 1);
      }

      expect(backend.pages.size).toBeGreaterThan(1);
      for (const pageNumber of backend.pages.keys()) {
        expect(pageNumber).toBeLessThan(0);
      }
    });
  });
});
