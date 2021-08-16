import { DataDocument, DataStoreBackend, StoreDocument } from "./data-store";
import { decodeBytes, encodeBytes } from "./encode-bytes";

// used to "mark" an encrypted store document and try to avoid accidental use as an unencrypted store document
const MARKER_ROOT_ID = "#ENCRYPTED#";

const subtleCrypto = crypto.subtle;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

interface CachedKey {
  encodedSalt: string;
  key: CryptoKey;
}

interface EncryptedProperties {
  data: string;
  salt: string;
  iv: string;
}

const encryptedPropertiesName = "encrypted";

function extractEncryptedProperties(storeDocument: StoreDocument): EncryptedProperties | undefined {
  const value = (storeDocument.extraProperties || {})[encryptedPropertiesName];
  if (typeof value === "object") {
    const data = value.data;
    const salt = value.salt;
    const iv = value.iv;
    if (typeof data !== "string" || typeof salt !== "string" || typeof iv !== "string") {
      throw new Error("invalid EncryptedProperties: " + JSON.stringify(value));
    }
    return { data, salt, iv };
  }
  return undefined;
}

function randomBytes(count: number): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(count));
}

function getAesGcmParams(iv: Uint8Array): AesGcmParams {
  if (iv.length != 12) {
    throw new Error("iv should be 96 bits for AES-GCM");
  }
  return {
    name: "AES-GCM",
    iv,
    tagLength: 128,
  };
}

// just use all zeros as the iv, since each encryption key (the random id) is only used once
const dataEncryptAesGcmParams = getAesGcmParams(new Uint8Array(12));

// just use all zeros as the iv, since each encryption key (the random id) is only used once
const idEncryptIv = new Uint8Array(16);

// encrypt one zero byte with the key from the random id, with the padding this will result in 16 encrypted bytes
const idEncryptData = new Uint8Array(1);

async function encryptDataDocumentId(dataDocumentId: string): Promise<string> {
  const idBytes = decodeBytes(dataDocumentId);
  if (idBytes.length !== 16) {
    throw new Error("unexpected idBytes length: " + idBytes.length);
  }

  const idEncryptKey = await subtleCrypto.importKey("raw", idBytes, "AES-CBC", false, ["encrypt"]);

  const encryptedIdBytes = await subtleCrypto.encrypt(
    { name: "AES-CBC", iv: idEncryptIv },
    idEncryptKey,
    idEncryptData
  );
  if (encryptedIdBytes.byteLength !== 16) {
    throw new Error("unexpected encryptedIdBytes length: " + encryptedIdBytes.byteLength);
  }

  return encodeBytes(new Uint8Array(encryptedIdBytes));
}

function getDataDocumentDataKey(dataDocumentId: string): Promise<CryptoKey> {
  return subtleCrypto.importKey("raw", decodeBytes(dataDocumentId), "AES-GCM", false, ["encrypt", "decrypt"]);
}

export class EncryptingDataStoreBackend implements DataStoreBackend {
  private cachedKey?: CachedKey;

  constructor(private readonly nextDataStoreBackend: DataStoreBackend, private readonly secret: string) {}

  private async getSecretKey(encodedSalt: string): Promise<CryptoKey> {
    if (this.cachedKey && this.cachedKey.encodedSalt === encodedSalt) {
      // use the cached key if possible since PBKDF2 is intentionally slow
      return this.cachedKey.key;
    }

    const salt = decodeBytes(encodedSalt);
    const rawKey = await subtleCrypto.importKey("raw", new TextEncoder().encode(this.secret), "PBKDF2", false, [
      "deriveKey",
    ]);

    const pbkdf2Params: Pbkdf2Params = {
      name: "PBKDF2",
      salt: salt,
      // just use 100000 for now, we could potentially make that configurable later
      iterations: 100000,
      hash: "SHA-256",
    };
    const key = await subtleCrypto.deriveKey(
      pbkdf2Params,
      rawKey,
      {
        name: "AES-GCM",
        length: 128,
      },
      false,
      ["encrypt", "decrypt"]
    );

    this.cachedKey = { encodedSalt, key };

    return key;
  }

  async getStoreDocument(): Promise<StoreDocument> {
    const encryptedStoreDocument = await this.nextDataStoreBackend.getStoreDocument();
    const encryptedProperties = extractEncryptedProperties(encryptedStoreDocument);
    if (encryptedProperties) {
      if (encryptedStoreDocument.rootId !== MARKER_ROOT_ID) {
        throw new Error("invalid encrypted store document, marker rootId is missing");
      }
      const key = await this.getSecretKey(encryptedProperties.salt);

      const decryptedData = await subtleCrypto.decrypt(
        getAesGcmParams(decodeBytes(encryptedProperties.iv)),
        key,
        decodeBytes(encryptedProperties.data)
      );
      const document: StoreDocument = JSON.parse(textDecoder.decode(decryptedData));

      // add the unencrypted parts to the document again
      document.id = encryptedStoreDocument.id;
      document.version = encryptedStoreDocument.version;
      (document.extraProperties ||= {})[encryptedPropertiesName] = encryptedProperties;

      return document;
    } else if (encryptedStoreDocument.rootId !== undefined) {
      // it seems to be an unencrypted store document
      throw new Error("unencrypted store document");
    } else {
      // seems to be a new store document, so just return it
      return encryptedStoreDocument;
    }
  }

  async getDataDocuments(dataDocumentIds: string[]): Promise<Record<string, DataDocument | undefined>> {
    const encryptedIds = await Promise.all(dataDocumentIds.map(encryptDataDocumentId));

    const encryptedDocuments = await this.nextDataStoreBackend.getDataDocuments(encryptedIds);

    const result: Record<string, DataDocument | undefined> = {};
    for (let i = 0; i < dataDocumentIds.length; ++i) {
      const encryptedDocument = encryptedDocuments[encryptedIds[i]];
      if (encryptedDocument) {
        const id = dataDocumentIds[i];

        const dataKey = await getDataDocumentDataKey(id);
        const dataBytes = await subtleCrypto.decrypt(
          dataEncryptAesGcmParams,
          dataKey,
          decodeBytes(encryptedDocument.data)
        );

        result[id] = {
          id,
          version: encryptedDocument.version,
          data: textDecoder.decode(dataBytes),
        };
      }
    }

    return result;
  }

  /**
   * Encrypt the ids here, so that it will not be possible to read the documents even though the data is not deleted
   * yet. And other clients with older store document versions will still be able to read and decrypt them.
   */
  async convertIdsToDeleteIds(dataDocumentIds: string[]): Promise<string[]> {
    const encryptedIds = await Promise.all(dataDocumentIds.map(encryptDataDocumentId));
    const nextBackendIds = this.nextDataStoreBackend.convertIdsToDeleteIds(encryptedIds);
    return nextBackendIds === undefined ? encryptedIds : nextBackendIds;
  }

  async update(
    newStoreDocument: StoreDocument,
    newDataDocuments: DataDocument[],
    obsoleteDataDocumentIds: string[]
  ): Promise<boolean> {
    const storeDocumentToEncrypt: StoreDocument = { ...newStoreDocument };

    // remove unencrypted entries
    delete storeDocumentToEncrypt.id;
    delete storeDocumentToEncrypt.version;
    if (storeDocumentToEncrypt.extraProperties) {
      storeDocumentToEncrypt.extraProperties = { ...storeDocumentToEncrypt.extraProperties };
      delete storeDocumentToEncrypt.extraProperties[encryptedPropertiesName];
      if (Object.keys(storeDocumentToEncrypt.extraProperties).length === 0) {
        delete storeDocumentToEncrypt.extraProperties;
      }
    }

    const newStoreDocumentIv = randomBytes(12);
    // generate a new random salt if it is the first write (i.e. encryptedProperties is undefined)
    const encodedSalt = extractEncryptedProperties(newStoreDocument)?.salt || encodeBytes(randomBytes(16));

    const encryptedStoreDocumentData = await subtleCrypto.encrypt(
      getAesGcmParams(newStoreDocumentIv),
      await this.getSecretKey(encodedSalt),
      textEncoder.encode(JSON.stringify(storeDocumentToEncrypt))
    );

    const newEncryptedProperties: EncryptedProperties = {
      data: encodeBytes(new Uint8Array(encryptedStoreDocumentData)),
      salt: encodedSalt,
      iv: encodeBytes(newStoreDocumentIv),
    };

    const encryptedNewStoreDocument: StoreDocument = {
      id: newStoreDocument.id,
      version: newStoreDocument.version,
      rootId: MARKER_ROOT_ID,
    };
    (encryptedNewStoreDocument.extraProperties ||= {})[encryptedPropertiesName] = newEncryptedProperties;

    const encryptedNewDataDocuments: DataDocument[] = [];
    for (const newDataDocument of newDataDocuments) {
      const id = newDataDocument.id!;
      const encryptedId = await encryptDataDocumentId(id);
      const dataKey = await getDataDocumentDataKey(id);

      const encryptedDataBytes = await subtleCrypto.encrypt(
        dataEncryptAesGcmParams,
        dataKey,
        textEncoder.encode(newDataDocument.data)
      );

      encryptedNewDataDocuments.push({
        id: encryptedId,
        version: newDataDocument.version,
        data: encodeBytes(new Uint8Array(encryptedDataBytes)),
      });
    }

    const result = await this.nextDataStoreBackend.update(
      encryptedNewStoreDocument,
      encryptedNewDataDocuments,
      obsoleteDataDocumentIds
    );

    if (result) {
      // update the version and the encrypted properties in the original document
      newStoreDocument.version = encryptedNewStoreDocument.version;
      (newStoreDocument.extraProperties ||= {})[encryptedPropertiesName] = newEncryptedProperties;

      // update the versions in the original newDataDocuments
      for (let i = 0; i < newDataDocuments.length; ++i) {
        newDataDocuments[i].version = encryptedNewDataDocuments[i].version;
      }
    }

    return result;
  }
}
