import { DBSchema, IDBPDatabase, openDB } from "idb";
import { DataDocument, DataStoreBackend, StoreDocument } from "./data-store";

interface CacheEntry {
  document: DataDocument;
  // TODO: maybe add something like "lastAccess" to be able to remove old/unused entries
}

interface Schema extends DBSchema {
  "data-document": { key: string; value: CacheEntry };
}

export class CachingDataStoreBackend implements DataStoreBackend {
  private opened = false;
  private openPromise: Promise<IDBPDatabase<Schema>> | undefined;
  private database: IDBPDatabase<Schema> | undefined;

  constructor(private readonly nextDataStoreBackend: DataStoreBackend, private readonly cacheKey: string) {}

  private async getDatabase(): Promise<IDBPDatabase<Schema> | undefined> {
    if (!this.opened) {
      try {
        this.openPromise ||= openDB<Schema>("CachingDataStoreBackend:" + this.cacheKey, 1, {
          upgrade(database) {
            database.createObjectStore("data-document");
          },
        });

        this.database = await this.openPromise;
      } catch (e) {
        console.log("failed to open IndexedDB: ", e);
      } finally {
        this.opened = true;
        this.openPromise = undefined;
      }
    }
    return this.database;
  }

  getStoreDocument(): Promise<StoreDocument> {
    // for now don't cache the StoreDocument
    return this.nextDataStoreBackend.getStoreDocument();
  }

  async getDataDocuments(dataDocumentIds: string[]): Promise<Record<string, DataDocument | undefined>> {
    if (dataDocumentIds.length === 0) {
      return {};
    }

    const cachedDocuments: Record<string, DataDocument> = {};
    let uncachedIds = dataDocumentIds;
    const database = await this.getDatabase();
    if (database) {
      const transaction = database.transaction("data-document");
      const store = transaction.objectStore("data-document");
      uncachedIds = [];
      for (const dataDocumentId of dataDocumentIds) {
        const cacheEntry = await store.get(dataDocumentId);
        if (cacheEntry) {
          cachedDocuments[dataDocumentId] = cacheEntry.document;
        } else {
          uncachedIds.push(dataDocumentId);
        }
      }
      await transaction.done;
    }

    if (uncachedIds.length === 0) {
      return cachedDocuments;
    }

    const backendResult = await this.nextDataStoreBackend.getDataDocuments(uncachedIds);

    if (database) {
      const transaction = database.transaction("data-document", "readwrite", { durability: "relaxed" });
      const store = transaction.objectStore("data-document");
      for (const dataDocumentId of uncachedIds) {
        const document = backendResult[dataDocumentId];
        if (document) {
          await store.put({ document }, dataDocumentId);
        }
      }
      await transaction.done;
    }

    return { ...cachedDocuments, ...backendResult };
  }

  convertIdsToDeleteIds(dataDocumentIds: string[]): Promise<string[]> | undefined {
    return this.nextDataStoreBackend.convertIdsToDeleteIds(dataDocumentIds);
  }

  async update(
    newStoreDocument: StoreDocument,
    newDataDocuments: DataDocument[],
    obsoleteDataDocumentIds: string[]
  ): Promise<boolean> {
    const success = await this.nextDataStoreBackend.update(newStoreDocument, newDataDocuments, obsoleteDataDocumentIds);

    if (success) {
      const database = await this.getDatabase();
      if (database) {
        const transaction = database.transaction("data-document", "readwrite", { durability: "relaxed" });
        const store = transaction.objectStore("data-document");
        for (const obsoleteDataDocumentId of obsoleteDataDocumentIds) {
          await store.delete(obsoleteDataDocumentId);
        }
        for (const newDataDocument of newDataDocuments) {
          await store.put({ document: newDataDocument }, newDataDocument.id);
        }
        await transaction.done;
      }
    }

    return success;
  }
}
