import { assert } from "shared-util";
import { DataTransformingPageStoreBackend } from "./DataTransformingPageStoreBackend";
import type { PageStoreBackend } from "./PageStoreBackend";

const subtleCrypto = crypto.subtle;

const ALGORITHM = "AES-GCM";

// 12 bytes for the initialization vector and 16 bytes for the authentication tag
const OVERHEAD = 28;

export class EncryptingPageStoreBackend extends DataTransformingPageStoreBackend {
  constructor(
    underlyingBackend: PageStoreBackend,
    private readonly key: CryptoKey,
  ) {
    super(underlyingBackend);

    // do some very basic validation of the key
    if (key.algorithm.name !== ALGORITHM) {
      throw new Error("algorithm must be " + ALGORITHM);
    }
  }

  get maxPageSizeOverhead(): number {
    return OVERHEAD;
  }

  protected async transform(data: Uint8Array): Promise<Uint8Array> {
    // generate a random 96 bits IV (as recommended for AES-GCM)
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const encryptedData = await subtleCrypto.encrypt(
      {
        name: ALGORITHM,
        iv,
        tagLength: 128,
      },
      this.key,
      data,
    );
    const result = new Uint8Array(iv.byteLength + encryptedData.byteLength);
    assert(result.length === data.length + OVERHEAD);
    result.set(iv, 0);
    result.set(new Uint8Array(encryptedData), iv.length);
    return result;
  }

  protected async reverseTransform(transformedData: Uint8Array): Promise<Uint8Array> {
    const iv = transformedData.subarray(0, 12);
    let decryptedData = await subtleCrypto.decrypt(
      {
        name: ALGORITHM,
        iv: iv,
      },
      this.key,
      transformedData.subarray(12, transformedData.length),
    );
    return new Uint8Array(decryptedData);
  }
}
