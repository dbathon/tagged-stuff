import { describe, assert, expect, test } from "vitest";
import { InMemoryPageStoreBackend } from "./InMemoryPageStoreBackend";
import { PageStore } from "./PageStore";
import { uint8ArraysEqual } from "shared-util";
import { CompressingPageStoreBackend } from "./CompressingPageStoreBackend";
import { EncryptingPageStoreBackend } from "./EncryptingPageStoreBackend";

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
  assert(uint8ArraysEqual(expected, array));
}

describe("PageStore", () => {
  describe("read from empty store", () => {
    test("should return pages with zero bytes", async () => {
      const backend = new InMemoryPageStoreBackend();
      const store = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);
      assert(store.loading === false);
      assert(store.getPage(0) === undefined);
      assert(store.getPage(2) === undefined);

      assert((store.loading as boolean) === true);
      await store.loadingFinished();
      assert(store.loading === false);

      const page0 = store.getPage(0);
      const page2 = store.getPage(2);
      assert(page0?.[0] === 0);
      assert(page0?.[1] === 0);
      assert(page0?.[5] === 0);
      assert(page0?.[store.pageSize - 1] === 0);
      assert(page0?.[store.pageSize] === undefined);

      assert(page2?.[5] === 0);
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

      assert(page0?.[0] === 0);
      assert(page0?.[1] === 0);
      assert(store2Page0?.[0] === 0);
      assert(store2Page0?.[1] === 0);

      const result = await store.runTransaction((pageAccess) => {
        pageAccess.getForUpdate(0)[0] = 42;
      });

      assert(result.committed === true);

      assert((page0?.[0] as number) === 42);
      assert(page0?.[1] === 0);
      assert(store2Page0?.[0] === 0);
      assert(store2Page0?.[1] === 0);

      store2.refresh();
      await store2.loadingFinished();
      assert((store2Page0?.[0] as number) === 42);
      assert(store2Page0?.[1] === 0);
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

      assert(uint8ArraysEqual(page0!, new Uint8Array(store.pageSize)));
      assert(uint8ArraysEqual(page1!, new Uint8Array(store.pageSize)));

      {
        const result = await store.runTransaction((pageAccess) => {
          pageAccess.getForUpdate(0)[0] = 42;
        });
        assert(result.committed === true);
      }
      assert(page0?.[0] === 42);

      // commit without retry in store2 should fail
      assert(store2Page0?.[0] === 0);
      {
        const result = await store2.runTransaction((pageAccess) => {
          pageAccess.getForUpdate(0)[0] = 43;
        }, 0);
        assert(result.committed === false);
      }
      assert(store2Page0?.[0] === 0);

      store2.refresh();
      await store2.loadingFinished();

      // 2nd try after refresh should work
      {
        const result = await store2.runTransaction((pageAccess) => {
          pageAccess.getForUpdate(0)[0] = 43;
        }, 0);
        assert(result.committed === true);
      }
      assert((store2Page0?.[0] as number) === 43);

      // and with retry should also work (back in the first store)
      assert(page0?.[0] === 42);
      const seenPrevValues: number[] = [];
      {
        const result = await store.runTransaction((pageAccess) => {
          const pageArray = pageAccess.getForUpdate(0);
          seenPrevValues.push(pageArray[0]);
          pageArray[0] = 44;
        });
        assert(result.committed === true);
      }
      assert((page0?.[0] as number) === 44);
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
      assert(result.committed === true);

      for (let i = 0; i < pageCount; i++) {
        expectEqualsFillRandom(store.getPage(i)!, writeCount, i + 1);
      }

      const store2 = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);
      for (let i = 0; i < pageCount; i++) {
        // trigger load
        store2.getPage(i);
        if (i === 0) {
          assert(store2.loading === true);
          await store2.loadingFinished();
        } else {
          // no more loading needed
          assert(store2.loading === false);
        }
        expectEqualsFillRandom(store2.getPage(i)!, writeCount, i + 1);
      }

      // load pages after the first page group, those should also not require more loading
      store2.getPage(40);
      store2.getPage(100);
      assert(store2.loading === false);

      // everything is stored in the index page
      assert(backend.pages.size === 0);
    });

    test("should work with more data, by materializing the patches to actual pages and also using encryption and compression", async () => {
      const inMemoryBackend = new InMemoryPageStoreBackend();
      const key = await crypto.subtle.generateKey(
        {
          name: "AES-GCM",
          length: 128,
        },
        true,
        ["encrypt", "decrypt"]
      );
      const backend = new CompressingPageStoreBackend(new EncryptingPageStoreBackend(inMemoryBackend, key));
      const store = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);

      const pageCount = 100;
      const writeCount = 20;

      const result = await store.runTransaction((pageAccess) => {
        for (let i = 0; i < pageCount; i++) {
          fillRandom(pageAccess.getForUpdate(i), writeCount, i + 1);
        }
      });
      assert(result.committed === true);

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

      assert(inMemoryBackend.pages.size > 1);
    });

    test("should work with more data per page", async () => {
      const backend = new InMemoryPageStoreBackend();
      const store = new PageStore(backend, PAGE_SIZE, PAGE_SIZE);

      const pageCount = 4;
      const writeCount = 2000;

      const result = await store.runTransaction((pageAccess) => {
        for (let i = 0; i < pageCount; i++) {
          fillRandom(pageAccess.getForUpdate(i), writeCount, i + 1);
        }
      });
      assert(result.committed === true);

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

      assert(backend.pages.size > 1);

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
      assert(result2.committed === true);

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
