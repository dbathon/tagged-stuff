import { Document } from "./document";
import { JdsClient } from "./jds-client";
import { BTreeModificationResult, BTreeNode, BTreeScanParameters, RemoteBTree } from "./remote-b-tree";
import { Result } from "./result";

export class ConflictError extends Error {
  constructor(readonly documentId: string, message?: string) {
    super(message);
  }
}

interface StoreDocument extends Document {
  rootId?: string;
}

interface DataDocument extends Document {
  data: string;
}

/** This number is slightly greater than 2 * 31, so about 32 bits */
const MAX_BASE36_6CHARS = parseInt("zzzzzz", 36);
function uint32ToStringForRandomId(input: number): string {
  const encodedNumber = input > MAX_BASE36_6CHARS ? input - MAX_BASE36_6CHARS : input;
  return encodedNumber.toString(36).padStart(6, "0");
}

const randomBuffer = new Uint32Array(4);
/**
 * @returns about 124 bits of randomness encoded with base36 in a string with length 24.
 */
function randomId(): string {
  crypto.getRandomValues(randomBuffer);
  return uint32ToStringForRandomId(randomBuffer[0])
    + uint32ToStringForRandomId(randomBuffer[1])
    + uint32ToStringForRandomId(randomBuffer[2])
    + uint32ToStringForRandomId(randomBuffer[3]);
}

class WriteOperation {
  newNodes?: Map<string, BTreeNode>;
  deletedNodes?: Map<string, BTreeNode>;
  rootId?: string;

  getNode(nodeId: string): BTreeNode | undefined {
    return this.newNodes?.get(nodeId);
  }

  apply(modificationResult: BTreeModificationResult) {
    this.rootId = modificationResult.newRootId;

    modificationResult.obsoleteNodes.forEach(node => {
      if (this.newNodes?.has(node.id)) {
        this.newNodes.delete(node.id);
      }
      else {
        if (!this.deletedNodes) {
          this.deletedNodes = new Map();
        }
        this.deletedNodes.set(node.id, node);
      }
    });

    modificationResult.newNodes.forEach(node => {
      if (!this.newNodes) {
        this.newNodes = new Map();
      }
      this.newNodes.set(node.id, node);

      if (this.deletedNodes?.has(node.id)) {
        // this should never happen, because we use random ids for new nodes
        throw new Error("new node has previously deleted id: " + node.id);
      }
    });
  }

  updateNodeCache(nodeCache: Map<string, BTreeNode>) {
    this.deletedNodes?.forEach(node => nodeCache.delete(node.id));
    this.newNodes?.forEach(node => nodeCache.set(node.id, node));
  }
}

class QueuedOperation {

  private completionPromise?: Promise<void>;
  private completionPromiseResolve?: () => void;

  constructor(readonly type: "R" | "W", readonly previousCompleted?: Promise<void>) { }

  get afterCompletion(): Promise<void> {
    if (!this.completionPromise) {
      this.completionPromise = new Promise(resolve => {
        this.completionPromiseResolve = resolve;
      });
    }
    return this.completionPromise;
  }

  complete() {
    if (this.completionPromiseResolve) {
      this.completionPromiseResolve();
    }
  }
}

const ID_SEPARATOR = "|";

class DocumentInfo {
  constructor(readonly id: string, readonly version: string, readonly remoteId: string) { }

  static parseFromKey(key: string): DocumentInfo {
    const [id, version, remoteId] = key.split(ID_SEPARATOR);
    if (!id || !version || !remoteId) {
      throw new Error("unexpected key: " + key);
    }
    return new DocumentInfo(id, version, remoteId);
  }

  buildKey() {
    return this.id + ID_SEPARATOR + this.version + ID_SEPARATOR + this.remoteId;
  }

  getNextVersion() {
    // TODO: maybe change this to something else..., maybe like in jds
    const oldVersionNumber = parseInt(this.version);
    if (oldVersionNumber !== oldVersionNumber) {
      throw new Error("unexpected version string: " + this.version);
    }
    return (oldVersionNumber + 1).toString();
  }
}

export class DataStore {

  private readonly jdsClient: JdsClient;

  private readonly tree: RemoteBTree;

  private readonly nodeCache = new Map<string, BTreeNode>();

  private lastQueuedOperation?: QueuedOperation;

  private activeReads = 0;
  private activeWriteOperation?: WriteOperation;

  constructor(jdsBaseUrl: string, readonly storeId: string) {
    this.jdsClient = new JdsClient(jdsBaseUrl);

    const getAndCacheNode = async (nodeId: string) => {
      const document: DataDocument = await this.jdsClient.get(nodeId);
      const node: BTreeNode = JSON.parse(document.data);
      this.nodeCache.set(nodeId, node);
      return node;
    };
    const fetchNode = (nodeId: string) => {
      const cachedNode = this.activeWriteOperation?.getNode(nodeId) || this.nodeCache.get(nodeId);
      if (cachedNode !== undefined) {
        return Result.withValue(cachedNode);
      }
      else {
        return Result.withPromise(getAndCacheNode(nodeId));
      }
    };
    // just use order 50 for now, TODO optimize this...
    this.tree = new RemoteBTree(50, fetchNode, randomId);
  }

  private queueIfNecessary(operationType: "R" | "W"): QueuedOperation {
    if (!this.lastQueuedOperation) {
      this.lastQueuedOperation = new QueuedOperation(operationType);
    }
    else if (operationType === "R" && this.lastQueuedOperation.type === "R") {
      // nothing to do, the reads can happen in parallel
    }
    else {
      this.lastQueuedOperation = new QueuedOperation(operationType, this.lastQueuedOperation.afterCompletion);
    }
    return this.lastQueuedOperation;
  }

  private completeOperation(queuedOperation: QueuedOperation) {
    queuedOperation.complete();
    if (queuedOperation === this.lastQueuedOperation) {
      this.lastQueuedOperation = undefined;
    }
  }

  private async readOperation<T>(body: () => Promise<T>): Promise<T> {
    const queuedOperation = this.queueIfNecessary("R");
    if (queuedOperation.previousCompleted) {
      await queuedOperation.previousCompleted;
    }
    if (this.activeWriteOperation || this.activeReads < 0) {
      // sanity check
      throw new Error("invalid state for read: " + this.activeWriteOperation + ", " + this.activeReads);
    }
    // multiple reads can be active at the same time
    ++this.activeReads;
    try {
      return await body();
    }
    finally {
      --this.activeReads;
      if (this.activeReads === 0) {
        this.completeOperation(queuedOperation);
      }
    }
  }

  private async writeOperation<T>(body: (writeOperation: WriteOperation) => Promise<T>): Promise<T> {
    const queuedOperation = this.queueIfNecessary("W");
    if (queuedOperation.previousCompleted) {
      await queuedOperation.previousCompleted;
    }
    if (this.activeWriteOperation || this.activeReads !== 0) {
      // sanity check
      throw new Error("invalid state for write: " + this.activeWriteOperation + ", " + this.activeReads);
    }
    this.activeWriteOperation = new WriteOperation();
    try {
      return await body(this.activeWriteOperation);
    }
    finally {
      this.activeWriteOperation = undefined;
      this.completeOperation(queuedOperation);
    }
  }

  // TODO: use Result once caching is implemented
  private async getStoreDocument(): Promise<StoreDocument> {
    const queryResult: StoreDocument[] = await this.jdsClient.query({ id: this.storeId });
    if (queryResult.length === 0) {
      // does not exist yet, return a new one
      return {
        id: this.storeId
      };
    }
    else {
      return queryResult[0];
    }
  }

  private getDocumentInfo(id: string, rootId: string | undefined): Result<DocumentInfo | undefined> {
    if (rootId === undefined) {
      return Result.withValue(undefined);
    }
    const keyPrefix = id + ID_SEPARATOR;
    return this.tree.scan(new BTreeScanParameters(1, keyPrefix), rootId).transform(keys => {
      if (keys.length === 0 || !keys[0].startsWith(keyPrefix)) {
        return undefined;
      }
      return DocumentInfo.parseFromKey(keys[0]);
    });
  }

  private validateId(id: string): string {
    if (id.indexOf(ID_SEPARATOR) >= 0) {
      throw new Error("'" + ID_SEPARATOR + "' is not allowed in document ids: " + id);
    }
    return id;
  }

  private async fetchDocuments<D extends Document>(documentInfos: DocumentInfo[]): Promise<D[]> {
    if (documentInfos.length === 0) {
      return [];
    }
    // fetch in batches fo 50 (jds has a limited result size and the URL gets too long and we can even fetch in parallel)
    let remoteIdsBatch: string[] = [];
    const promises: Promise<DataDocument[]>[] = [];
    for (const documentInfo of documentInfos) {
      remoteIdsBatch.push(documentInfo.remoteId);
      if (remoteIdsBatch.length >= 50) {
        promises.push(this.jdsClient.query<DataDocument>({ id: { in: remoteIdsBatch } }));
        remoteIdsBatch = [];
      }
    }
    // handle the last batch
    if (remoteIdsBatch.length > 0) {
      promises.push(this.jdsClient.query<DataDocument>({ id: { in: remoteIdsBatch } }));
    }

    const batchResults = await Promise.all(promises);

    const remoteIdToDocument = new Map<string, D>();
    for (const batchResult of batchResults) {
      for (const dataDocument of batchResult) {
        remoteIdToDocument.set(dataDocument.id!, JSON.parse(dataDocument.data));
      }
    }

    return documentInfos.map(documentInfo => {
      const document = remoteIdToDocument.get(documentInfo.remoteId);
      if (document === undefined) {
        throw new ConflictError("remote document not found: " + documentInfo.id + ", " + documentInfo.remoteId);
      }
      // restore id and version in the document
      document.id = documentInfo.id;
      document.version = documentInfo.version;
      return document;
    });
  }

  get<D extends Document>(id: string): Promise<D | undefined> {
    return this.readOperation(async () => {
      const rootId = (await this.getStoreDocument()).rootId;
      return this.getDocumentInfo(id, rootId).transform(documentInfo => {
        if (documentInfo === undefined) {
          return undefined;
        }
        return Result.withPromise(this.fetchDocuments<D>([documentInfo]))
          .transform(dataDocuments => dataDocuments[0]);
      }).toPromise();
    });
  }

  scan<D extends Document>(maxResults: number | undefined, minId?: string, maxIdExclusive?: string): Promise<D[]> {
    return this.readOperation(async () => {
      const rootId = (await this.getStoreDocument()).rootId;
      if (rootId === undefined) {
        return [];
      }

      return this.tree.scan(new BTreeScanParameters(maxResults,
        minId === undefined ? undefined : this.validateId(minId),
        maxIdExclusive === undefined ? undefined : this.validateId(maxIdExclusive)), rootId)
        .transform(keys => {
          if (keys.length === 0) {
            return [];
          }
          const documentInfos = keys.map(key => {
            const documentInfo = DocumentInfo.parseFromKey(key);
            if ((minId !== undefined && documentInfo.id < minId)
              || (maxIdExclusive !== undefined && maxIdExclusive <= documentInfo.id)) {
              // this should not happen
              throw new Error("got unexpected id: " + documentInfo.id + ", " + minId + ", " + maxIdExclusive);
            }
            return documentInfo;
          });
          return Result.withPromise(this.fetchDocuments<D>(documentInfos));
        })
        .toPromise();
    });
  }


  private getId(document: Document): string {
    if (document.id === undefined) {
      throw new Error("document id must be set: " + document);
    }
    this.validateId(document.id);
    return document.id;
  }

  putAndDelete(parameters: { put?: Document[], delete?: Document[]; }): Promise<void> {
    return this.writeOperation(async writeOperation => {
      if ((!parameters.put || parameters.put.length === 0)
        && (!parameters.delete || parameters.delete.length === 0)) {
        // nothing to do
        return;
      }
      const storeDocument = await this.getStoreDocument();
      writeOperation.rootId = storeDocument.rootId;

      const treeInserts: string[] = [];
      const treeDeletions: string[] = [];
      const putDocuments: Document[] = [];
      const deleteDocuments: Document[] = [];

      const successActions: (() => void)[] = [];

      for (const { isPut, documents } of [
        { isPut: true, documents: parameters.put },
        { isPut: false, documents: parameters.delete },
      ]) {
        if (documents) {
          for (const document of documents) {
            const id = this.getId(document);
            const documentInfoResult = this.getDocumentInfo(id, writeOperation.rootId);
            let documentInfo: DocumentInfo | undefined;
            if (documentInfoResult.hasValue) {
              documentInfo = documentInfoResult.value;
            }
            else {
              documentInfo = await documentInfoResult.promise;
            }

            let newVersion: string | undefined;
            if (documentInfo) {
              if (documentInfo.version !== document.version) {
                throw new ConflictError(id, "version does not match: expected " + documentInfo.version + ", but it is " + document.version);
              }

              // TODO: maybe only update if the document actually changed..., that would require loading the old one or some hash over the document...
              deleteDocuments.push({ id: documentInfo.remoteId });
              treeDeletions.push(documentInfo.buildKey());
              newVersion = documentInfo.getNextVersion();
            }
            else {
              if (document.version !== undefined) {
                throw new ConflictError(id, "document does not exist, given version: " + document.version);
              }
              if (!isPut) {
                throw new ConflictError(id, "cannot delete document without version");
              }
              newVersion = "0";
            }

            if (isPut) {
              const newDocument: Document = { ...document };
              // do not save the id and version in the remote document (it is part of the key anyway)
              newDocument.id = undefined;
              newDocument.version = undefined;

              const newRemoteId = randomId();
              const newDataDocument: DataDocument = {
                id: newRemoteId,
                data: JSON.stringify(newDocument)
              };

              putDocuments.push(newDataDocument);
              treeInserts.push(new DocumentInfo(id, newVersion, newRemoteId).buildKey());

              successActions.push(() => document.version = newVersion);
            }
          }
        }
      }

      if (writeOperation.rootId === undefined) {
        writeOperation.apply(this.tree.initializeNewTree());
      }

      for (const { isInsert, keys } of [
        { isInsert: true, keys: treeInserts },
        { isInsert: false, keys: treeDeletions },
      ]) {
        for (const key of keys) {
          const oldRootId = writeOperation.rootId!;
          const modificationResultResult = isInsert ?
            this.tree.insertKey(key, oldRootId) :
            this.tree.deleteKey(key, oldRootId);

          const modificationResult = modificationResultResult.hasValue ?
            modificationResultResult.value :
            await modificationResultResult.promise;

          writeOperation.apply(modificationResult);
          if (oldRootId === writeOperation.rootId) {
            throw new Error("no modification for " + (isInsert ? "insert" : "delete") + " of: " + key);
          }
        }
      }

      writeOperation.newNodes?.forEach(node => {
        const newNodeDataDocument: DataDocument = {
          id: node.id,
          data: JSON.stringify(node)
        };
        putDocuments.push(newNodeDataDocument);
      });

      writeOperation.deletedNodes?.forEach(node => {
        deleteDocuments.push({ id: node.id });
      });


      const newStoreDocument: StoreDocument = {
        ...storeDocument,
        rootId: writeOperation.rootId
      };
      putDocuments.push(newStoreDocument);

      const result = await this.jdsClient.multiPutAndDelete({
        put: putDocuments,
        delete: deleteDocuments
      });

      if (result.errorDocumentId !== undefined) {
        // something failed
        if (result.errorDocumentId === storeDocument.id) {
          // TODO: retry a few times...
          throw new ConflictError("", "the storeDocument was updated concurrently, please try again");
        }
        else {
          throw new Error("an unexpeceted put or delete failed: " + result.errorDocumentId);
        }
      }
      else {
        // everything was successful
        writeOperation.updateNodeCache(this.nodeCache);
        successActions.forEach(action => action());
      }
    });
  };

  put(...documents: Document[]): Promise<void> {
    return this.putAndDelete({ put: documents });
  };

  delete(...documents: Document[]): Promise<void> {
    return this.putAndDelete({ delete: documents });
  };

  // TODO: "transactions"/batched pust/deletes, commit, reset, "refresh"/"cached" StoreDocument

}
