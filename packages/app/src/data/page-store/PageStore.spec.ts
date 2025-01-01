import { describe, expect, test } from "vitest";
import { InMemoryPageStoreBackend } from "./InMemoryPageStoreBackend";
import { PageStore } from "./PageStore";
import { uint8ArraysEqual } from "shared-util";

const PAGE_SIZE = 8192;

function xorShift32(x: number): number {
  /* Algorithm "xor" from p. 4 of Marsaglia, "Xorshift RNGs" */
  x ^= x << 13;
  x ^= x >> 17;
  x ^= x << 5;
  return x;
}

function fillRandom(array: Uint8Array, writes: number, seed: number) {
  let x = seed === 0 ? 1 : seed;
  const maxOffset = array.byteLength - 4;
  for (let i = 0; i < writes; i++) {
    x = xorShift32(x);
    const offset = Math.abs(x) % (maxOffset + 1);
    array[offset] = x >>> 24;
    array[offset + 1] = x >>> 16;
    array[offset + 2] = x >>> 8;
    array[offset + 3] = x;
  }
}

function expectEqualsFillRandom(array: Uint8Array, writes: number, seed: number) {
  const expected = new Uint8Array(array.byteLength);
  fillRandom(expected, writes, seed);
  expect(uint8ArraysEqual(expected, array)).toBe(true);
}

describe("PageStore", () => {
  describe("read from empty store", () => {
    test("should return pages with zero bytes", async () => {
      const backend = new InMemoryPageStoreBackend();
      const store = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);
      expect(store.loading).toBe(false);
      expect(store.getPage(0)).toBe(undefined);
      expect(store.getPage(2)).toBe(undefined);

      expect(store.loading).toBe(true);
      await store.loadingFinished();
      expect(store.loading).toBe(false);

      const page0 = store.getPage(0);
      const page2 = store.getPage(2);
      expect(page0?.[0]).toBe(0);
      expect(page0?.[1]).toBe(0);
      expect(page0?.[5]).toBe(0);
      expect(page0?.[store.pageSize - 1]).toBe(0);
      expect(page0?.[store.pageSize]).toBe(undefined);

      expect(page2?.[5]).toBe(0);
    });
  });

  describe("constructor page size validations", () => {
    test("works", () => {
      [2, (1 << 12) - 1, (1 << 16) + 1, 1 << 17].forEach((pageSize) => {
        expect(() => new PageStore(new InMemoryPageStoreBackend(), pageSize, pageSize)).toThrow();
      });
      [1 << 12, 1 << 16].forEach((pageSize) => {
        expect(() => new PageStore(new InMemoryPageStoreBackend(), pageSize, pageSize)).not.toThrow();
      });
    });
  });

  describe("transaction", () => {
    test("should work if there are no conflicts", async () => {
      const backend = new InMemoryPageStoreBackend();
      const store = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);
      // trigger load
      store.getPage(0);
      await store.loadingFinished();

      const store2 = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);
      // trigger load
      store2.getPage(0);
      await store2.loadingFinished();

      const page0 = store.getPage(0);
      const store2Page0 = store2.getPage(0);

      expect(page0?.[0]).toBe(0);
      expect(page0?.[1]).toBe(0);
      expect(store2Page0?.[0]).toBe(0);
      expect(store2Page0?.[1]).toBe(0);

      const result = await store.runTransaction((pageAccess) => {
        pageAccess.getForUpdate(0)[0] = 42;
      });

      expect(result.committed).toBe(true);

      expect(page0?.[0]).toBe(42);
      expect(page0?.[1]).toBe(0);
      expect(store2Page0?.[0]).toBe(0);
      expect(store2Page0?.[1]).toBe(0);

      store2.refresh();
      await store2.loadingFinished();
      expect(store2Page0?.[0]).toBe(42);
      expect(store2Page0?.[1]).toBe(0);
    });

    test("should fail if there are conflicts", async () => {
      const backend = new InMemoryPageStoreBackend();
      const store = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);
      // trigger load
      store.getPage(0);
      store.getPage(1);
      await store.loadingFinished();

      const store2 = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);
      // trigger load
      store2.getPage(0);
      await store2.loadingFinished();

      const page0 = store.getPage(0);
      const page1 = store.getPage(1);
      const store2Page0 = store2.getPage(0);

      expect(uint8ArraysEqual(page0!, new Uint8Array(store.pageSize))).toBe(true);
      expect(uint8ArraysEqual(page1!, new Uint8Array(store.pageSize))).toBe(true);

      {
        const result = await store.runTransaction((pageAccess) => {
          pageAccess.getForUpdate(0)[0] = 42;
        });
        expect(result.committed).toBe(true);
      }
      expect(page0?.[0]).toBe(42);

      // commit without retry in store2 should fail
      expect(store2Page0?.[0]).toBe(0);
      {
        const result = await store2.runTransaction((pageAccess) => {
          pageAccess.getForUpdate(0)[0] = 43;
        }, 0);
        expect(result.committed).toBe(false);
      }
      expect(store2Page0?.[0]).toBe(0);

      store2.refresh();
      await store2.loadingFinished();

      // 2nd try after refresh should work
      {
        const result = await store2.runTransaction((pageAccess) => {
          pageAccess.getForUpdate(0)[0] = 43;
        }, 0);
        expect(result.committed).toBe(true);
      }
      expect(store2Page0?.[0]).toBe(43);

      // and with retry should also work (back in the first store)
      expect(page0?.[0]).toBe(42);
      const seenPrevValues: number[] = [];
      {
        const result = await store.runTransaction((pageAccess) => {
          const pageArray = pageAccess.getForUpdate(0);
          seenPrevValues.push(pageArray[0]);
          pageArray[0] = 44;
        });
        expect(result.committed).toBe(true);
      }
      expect(page0?.[0]).toBe(44);
      expect(seenPrevValues).toStrictEqual([42, 43]);
    });

    test("should work with more data, but still only index page", async () => {
      const backend = new InMemoryPageStoreBackend();
      const store = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);

      const pageCount = 10;
      const writeCount = 20;

      const result = await store.runTransaction((pageAccess) => {
        for (let i = 0; i < pageCount; i++) {
          fillRandom(pageAccess.getForUpdate(i), writeCount, i + 1);
        }
      });
      expect(result.committed).toBe(true);

      for (let i = 0; i < pageCount; i++) {
        expectEqualsFillRandom(store.getPage(i)!, writeCount, i + 1);
      }

      const store2 = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);
      for (let i = 0; i < pageCount; i++) {
        // trigger load
        store2.getPage(i);
        if (i === 0) {
          expect(store2.loading).toBe(true);
          await store2.loadingFinished();
        } else {
          // no more loading needed
          expect(store2.loading).toBe(false);
        }
        expectEqualsFillRandom(store2.getPage(i)!, writeCount, i + 1);
      }

      // load pages after the first page group, those should also not require more loading
      store2.getPage(40);
      store2.getPage(100);
      expect(store2.loading).toBe(false);

      // everything is stored in the index page
      expect(backend.pages.size).toBe(0);
    });

    test("should work with even more data, by moving the diffs to to the page group pages", async () => {
      const backend = new InMemoryPageStoreBackend();
      const store = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);

      const pageCount = 100;
      const writeCount = 20;

      const result = await store.runTransaction((pageAccess) => {
        for (let i = 0; i < pageCount; i++) {
          fillRandom(pageAccess.getForUpdate(i), writeCount, i + 1);
        }
      });
      expect(result.committed).toBe(true);

      for (let i = 0; i < pageCount; i++) {
        expectEqualsFillRandom(store.getPage(i)!, writeCount, i + 1);
      }

      const store2 = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);
      for (let i = 0; i < pageCount; i++) {
        // trigger load if necessary
        store2.getPage(i);
        await store2.loadingFinished();
        expectEqualsFillRandom(store2.getPage(i)!, writeCount, i + 1);
      }

      expect(backend.pages.size).toBeGreaterThan(1);
      for (const pageNumber of backend.pages.keys()) {
        expect(pageNumber).toBeLessThan(0);
      }
    });

    test("should work with more data in individual pages, by moving the data to actual pages", async () => {
      const backend = new InMemoryPageStoreBackend();
      const store = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);

      const pageCount = 4;
      const writeCount = 2000;

      const result = await store.runTransaction((pageAccess) => {
        for (let i = 0; i < pageCount; i++) {
          fillRandom(pageAccess.getForUpdate(i), writeCount, i + 1);
        }
      });
      expect(result.committed).toBe(true);

      for (let i = 0; i < pageCount; i++) {
        expectEqualsFillRandom(store.getPage(i)!, writeCount, i + 1);
      }

      const store2 = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);
      for (let i = 0; i < pageCount; i++) {
        // trigger load if necessary
        store2.getPage(i);
        await store2.loadingFinished();
        expectEqualsFillRandom(store2.getPage(i)!, writeCount, i + 1);
      }

      expect(backend.pages.size).toBeGreaterThan(1);
      expect([...backend.pages.keys()].filter((key) => key >= 0).length).toBeGreaterThan(0);

      // overwrite pages with new data and check again
      const result2 = await store.runTransaction((pageAccess) => {
        for (let i = 0; i < pageCount; i++) {
          // clear the previous data
          const array = pageAccess.getForUpdate(i);
          for (let i = 0; i < array.length; i++) {
            array[i] = 0;
          }
          fillRandom(pageAccess.getForUpdate(i), writeCount, i + 1000);
        }
      });
      expect(result2.committed).toBe(true);

      store2.refresh();
      for (let i = 0; i < pageCount; i++) {
        // trigger load if necessary
        store2.getPage(i);
        await store2.loadingFinished();
        expectEqualsFillRandom(store2.getPage(i)!, writeCount, i + 1000);
      }
    });
  });
});
