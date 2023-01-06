import { deepStrictEqual, doesNotThrow, strictEqual, throws } from "assert";
import { describe, it } from "mocha";
import { InMemoryPageStoreBackend } from "./InMemoryPageStoreBackend";
import { PageStore } from "./PageStore";

const PAGE_SIZE = 8192;

describe("PageStore", () => {
  describe("read from empty store", () => {
    it("should return pages with zero bytes", async () => {
      const backend = new InMemoryPageStoreBackend(PAGE_SIZE);
      const store = new PageStore(backend);
      strictEqual(store.loading, false);
      const page0 = store.getPage(0);
      const page2 = store.getPage(2);
      strictEqual(page0.value, undefined);
      strictEqual(page2.value, undefined);

      strictEqual(store.loading, true);
      await store.loadingFinished();
      strictEqual(store.loading, false);

      strictEqual(page0.value!.array[0], 0);
      strictEqual(page0.value!.array[1], 0);
      strictEqual(page0.value!.array[5], 0);
      strictEqual(page0.value!.array[PAGE_SIZE - 1], 0);
      strictEqual(page0.value!.array[PAGE_SIZE], undefined);

      strictEqual(page2.value!.array[5], 0);
    });
  });

  describe("constructor page size validations", () => {
    it("works", () => {
      [2, (1 << 12) - 1, (1 << 16) + 1, 1 << 17].forEach((pageSize) => {
        throws(() => new PageStore(new InMemoryPageStoreBackend(pageSize)));
      });
      [1 << 12, 1 << 16].forEach((pageSize) => {
        doesNotThrow(() => new PageStore(new InMemoryPageStoreBackend(pageSize)));
      });
    });
  });

  describe("transaction", () => {
    it("should work if there are no conflicts", async () => {
      const backend = new InMemoryPageStoreBackend(PAGE_SIZE);
      const store = new PageStore(backend);
      const page0 = store.getPage(0);
      await store.loadingFinished();

      const store2 = new PageStore(backend);
      const store2Page0 = store2.getPage(0);
      await store2.loadingFinished();

      strictEqual(page0.value?.array[0], 0);
      strictEqual(page0.value?.array[1], 0);
      strictEqual(store2Page0.value?.array[0], 0);
      strictEqual(store2Page0.value?.array[1], 0);

      const result = await store.runTransaction(() => {
        store.getPageDataForUpdate(0).array[0] = 42;

        strictEqual(page0.value?.array[0], 42);
        strictEqual(page0.value?.array[1], 0);
        strictEqual(store2Page0.value?.array[0], 0);
        strictEqual(store2Page0.value?.array[1], 0);
      });

      strictEqual(result.committed, true);

      strictEqual(page0.value?.array[0], 42);
      strictEqual(page0.value?.array[1], 0);
      strictEqual(store2Page0.value?.array[0], 0);
      strictEqual(store2Page0.value?.array[1], 0);

      store2.refresh();
      await store2.loadingFinished();
      strictEqual(store2Page0.value?.array[0], 42);
      strictEqual(store2Page0.value?.array[1], 0);
    });

    it("should fail if there are conflicts", async () => {
      const backend = new InMemoryPageStoreBackend(PAGE_SIZE);
      const store = new PageStore(backend);
      const page0 = store.getPage(0);
      await store.loadingFinished();

      const store2 = new PageStore(backend);
      const store2Page0 = store2.getPage(0);
      await store2.loadingFinished();

      strictEqual(page0.value?.array[0], 0);
      {
        const result = await store.runTransaction(() => {
          store.getPageDataForUpdate(0).array[0] = 42;

          strictEqual(page0.value?.array[0], 42);
        });
        strictEqual(result.committed, true);
      }
      strictEqual(page0.value?.array[0], 42);

      strictEqual(store2Page0.value?.array[0], 0);
      {
        const result = await store2.runTransaction(() => {
          store2.getPageDataForUpdate(0).array[0] = 43;

          strictEqual(store2Page0.value?.array[0], 43);
        });
        strictEqual(result.committed, false);
      }
      strictEqual(store2Page0.value?.array[0], 42);

      // 2nd try should work
      {
        const result = await store2.runTransaction(() => {
          store2.getPageDataForUpdate(0).array[0] = 43;

          strictEqual(store2Page0.value?.array[0], 43);
        });
        strictEqual(result.committed, true);
      }
      strictEqual(store2Page0.value?.array[0], 43);

      // and with retry should also work (back in the first store)
      strictEqual(page0.value?.array[0], 42);
      const seenPrevValues: number[] = [];
      {
        const result = await store.runTransaction(() => {
          const pageData = store.getPageDataForUpdate(0);
          seenPrevValues.push(pageData.array[0]);
          pageData.array[0] = 44;

          strictEqual(page0.value?.array[0], 44);
        }, 1);
        strictEqual(result.committed, true);
      }
      strictEqual(page0.value?.array[0], 44);
      deepStrictEqual(seenPrevValues, [42, 43]);
    });
  });
});
