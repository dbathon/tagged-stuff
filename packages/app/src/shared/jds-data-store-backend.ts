import { type DataDocument, type DataStoreBackend, type StoreDocument } from "./data-store";
import { JdsClient } from "./jds-client";

export class JdsDataStoreBackend implements DataStoreBackend {
  readonly jdsClient: JdsClient;

  constructor(
    jdsBaseUrl: string,
    readonly storeId: string,
  ) {
    this.jdsClient = new JdsClient(jdsBaseUrl);
  }

  async getStoreDocument(): Promise<StoreDocument> {
    const queryResult: StoreDocument[] = await this.jdsClient.query({ id: this.storeId });
    if (queryResult.length === 0) {
      // does not exist yet, return a new one
      return {
        id: this.storeId,
      };
    } else {
      return queryResult[0];
    }
  }

  async getDataDocuments(dataDocumentIds: string[]): Promise<Record<string, DataDocument | undefined>> {
    if (dataDocumentIds.length === 0) {
      return {};
    }
    // fetch in batches fo 50 (jds has a limited result size and the URL gets too long and we can even fetch in parallel)
    let idsBatch: string[] = [];
    const promises: Promise<DataDocument[]>[] = [];
    for (const dataDocumentId of dataDocumentIds) {
      idsBatch.push(dataDocumentId);
      if (idsBatch.length >= 50) {
        promises.push(this.jdsClient.query<DataDocument>({ id: { in: idsBatch } }));
        idsBatch = [];
      }
    }
    // handle the last batch
    if (idsBatch.length > 0) {
      promises.push(this.jdsClient.query<DataDocument>({ id: { in: idsBatch } }));
    }

    const batchResults = await Promise.all(promises);

    const result: Record<string, DataDocument | undefined> = {};
    for (const batchResult of batchResults) {
      for (const dataDocument of batchResult) {
        if (dataDocument.id === undefined) {
          throw new Error("dataDocument.id is undefined unexpectedly: " + JSON.stringify(dataDocument));
        }
        result[dataDocument.id] = dataDocument;
      }
    }
    return result;
  }

  convertIdsToDeleteIds(dataDocumentIds: string[]): undefined {
    return undefined;
  }

  async update(
    newStoreDocument: StoreDocument,
    newDataDocuments: DataDocument[],
    obsoleteDataDocumentIds: string[],
  ): Promise<boolean> {
    if (newStoreDocument.id !== this.storeId) {
      throw new Error("unexpected store document id: " + this.storeId + ", " + JSON.stringify(newStoreDocument));
    }

    const jdsResult = await this.jdsClient.multiPutAndDelete({
      put: [...newDataDocuments, newStoreDocument],
      delete: obsoleteDataDocumentIds.map((id) => ({ id })),
    });

    if (jdsResult.errorDocumentId !== undefined) {
      if (jdsResult.errorDocumentId === this.storeId) {
        return false;
      } else {
        throw new Error("update failed with unexpected errorDocumentId: " + jdsResult.errorDocumentId);
      }
    }

    if (jdsResult.newVersions === undefined) {
      throw new Error("jdsResult.newVersions is undefined unexpectedly");
    }
    // update versions in documents
    newStoreDocument.version = jdsResult.newVersions[newStoreDocument.id];
    for (const dataDocument of newDataDocuments) {
      dataDocument.version = jdsResult.newVersions[dataDocument.id!];
    }

    return true;
  }
}
