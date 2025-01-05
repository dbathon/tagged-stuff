import { DataTransformingPageStoreBackend } from "./DataTransformingPageStoreBackend";
import type { PageStoreBackend } from "./PageStoreBackend";

const NO_COMPRESSION = 0;
const GZIP_COMPRESSION = 1;

async function compress(data: Uint8Array): Promise<Uint8Array> {
  const arrayBuffer = await new Response(
    new Blob([data]).stream().pipeThrough(new CompressionStream("gzip"))
  ).arrayBuffer();
  if (arrayBuffer.byteLength <= data.byteLength) {
    // this should be the very common case
    const result = new Uint8Array(arrayBuffer.byteLength + 1);
    result.set(new Uint8Array(arrayBuffer), 0);
    result[result.length - 1] = GZIP_COMPRESSION;
    return result;
  } else {
    // this should happen very rarely, just use the uncompressed data if it is not compressible
    const result = new Uint8Array(data.length + 1);
    result.set(data, 0);
    result[result.length - 1] = NO_COMPRESSION;
    return result;
  }
}

async function decompress(data: Uint8Array): Promise<Uint8Array> {
  const type = data[data.length - 1];
  if (type === NO_COMPRESSION) {
    return data.subarray(0, data.length - 1);
  } else if (type === GZIP_COMPRESSION) {
    const arrayBuffer = await new Response(
      new Blob([data.subarray(0, data.length - 1)]).stream().pipeThrough(new DecompressionStream("gzip"))
    ).arrayBuffer();
    return new Uint8Array(arrayBuffer);
  } else {
    throw new Error("unexpected compression type: " + type);
  }
}

export class CompressingPageStoreBackend extends DataTransformingPageStoreBackend {
  constructor(underlyingBackend: PageStoreBackend) {
    super(underlyingBackend);
  }

  get maxPageSizeOverhead(): number {
    // we need one byte for the marker byte
    return 1;
  }

  protected transform(data: Uint8Array): Promise<Uint8Array> {
    return compress(data);
  }

  protected reverseTransform(transformedData: Uint8Array): Promise<Uint8Array> {
    return decompress(transformedData);
  }
}
