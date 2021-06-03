import { Document } from "./document";
import { encodeBytes } from "./encode-bytes";
import { BTreeModificationResult, BTreeNode, BTreeScanParameters, RemoteBTree } from "./remote-b-tree";
import { Result } from "./result";

export class ConflictError extends Error {
  constructor(readonly documentId: string, message?: string) {
    super(message);
  }
}

export interface StoreDocument extends Document {
  rootId?: string;
}

export interface DataDocument extends Document {
  data: string;
}

export interface DataStoreBackend {

  /**
   * @returns the current version of the StoreDocument (never undefined) as a Promise
   */
  getStoreDocument(): Promise<StoreDocument>;

  /**
   * @param dataDocumentIds the ids of DataDocuments to get
   * @returns the DataDocuments that were found/exist as a Promise
   */
  getDataDocuments(dataDocumentIds: string[]): Promise<Record<string, DataDocument | undefined>>;

  /**
   * Tries to perform an update of the StoreDocument and all the given DataDocuments. If it is successful then true is
   * returned, if the update is not possible because the StoreDocument is based on an old version then false is
   * returned. In all other cases (e.g. data documents cannot be created/deleted or some IO errors or anything else
   * that is unexpected) an Error is thrown.
   *
   * @param newStoreDocument
   * @param newDataDocuments
   * @param obsoleteDataDocumentIds
   * @returns whether the update was performed as a Promise
   */
  update(newStoreDocument: StoreDocument, newDataDocuments: DataDocument[], obsoleteDataDocumentIds: string[]): Promise<boolean>;

}

const randomBuffer = new Uint8Array(16);
/**
 * @returns 128 bits of randomness encoded with base64 in a string with length 22
 */
function randomId(): string {
  crypto.getRandomValues(randomBuffer);
  return encodeBytes(randomBuffer);
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
  constructor(readonly id: string, readonly version: string, readonly backendId: string) { }

  static parseFromKey(key: string): DocumentInfo {
    const [id, version, backendId] = key.split(ID_SEPARATOR);
    if (!id || !version || !backendId) {
      throw new Error("unexpected key: " + key);
    }
    return new DocumentInfo(id, version, backendId);
  }

  buildKey() {
    return this.id + ID_SEPARATOR + this.version + ID_SEPARATOR + this.backendId;
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

  private readonly tree: RemoteBTree;

  private readonly nodeCache = new Map<string, BTreeNode>();

  private lastQueuedOperation?: QueuedOperation;

  private activeReads = 0;
  private activeWriteOperation?: WriteOperation;

  constructor(private readonly backend: DataStoreBackend) {
    const getAndCacheNode = async (nodeId: string) => {
      const document = (await backend.getDataDocuments([nodeId]))[nodeId];
      if (document === undefined) {
        throw new Error("node not found: " + nodeId);
      }
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

  private getStoreDocument(): Promise<StoreDocument> {
    return this.backend.getStoreDocument();
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

    const getResult = await this.backend.getDataDocuments(documentInfos.map(documentInfo => documentInfo.backendId));

    return documentInfos.map(documentInfo => {
      const dataDocument = getResult[documentInfo.backendId];
      if (dataDocument === undefined) {
        throw new ConflictError(documentInfo.id, "backend document not found: " + documentInfo.backendId);
      }
      const document: D = JSON.parse(dataDocument.data);
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
      const putDocuments: DataDocument[] = [];
      const deleteDocumentIds: string[] = [];

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
              deleteDocumentIds.push(documentInfo.backendId);
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
              // do not save the id and version in the backend document (it is part of the key anyway)
              newDocument.id = undefined;
              newDocument.version = undefined;

              const newBackendId = randomId();
              const newDataDocument: DataDocument = {
                id: newBackendId,
                data: JSON.stringify(newDocument)
              };

              putDocuments.push(newDataDocument);
              treeInserts.push(new DocumentInfo(id, newVersion, newBackendId).buildKey());

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
        deleteDocumentIds.push(node.id);
      });


      const newStoreDocument: StoreDocument = {
        ...storeDocument,
        rootId: writeOperation.rootId
      };

      const success = await this.backend.update(newStoreDocument, putDocuments, deleteDocumentIds);

      if (!success) {
        // TODO: retry a few times...
        throw new ConflictError("", "the storeDocument was updated concurrently, please try again");
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
