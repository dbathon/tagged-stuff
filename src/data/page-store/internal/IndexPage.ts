import { MetaPageWithPatches } from "./MetaPageWithPatches";
import { PageGroupPage, pageNumberToPageGroupNumber } from "./PageGroupPage";
import { Patch } from "./Patch";
import { readUint48FromDataView, writeUint48toDataView } from "./util";

function readUint32ToUint48Map(view: DataView, startOffset: number, target: Map<number, number>): number {
  let offset = startOffset;
  const count = view.getUint16(offset);
  offset += 2;
  for (let i = 0; i < count; i++) {
    const key = view.getUint32(offset);
    offset += 4;
    const value = readUint48FromDataView(view, offset);
    offset += 6;
    target.set(key, value);
  }
  return offset;
}

function writeUint32ToUint48Map(view: DataView, startOffset: number, source: Map<number, number>): number {
  let offset = startOffset;
  view.setUint16(offset, source.size);
  offset += 2;
  source.forEach((value, key) => {
    view.setUint32(offset, key);
    offset += 4;
    writeUint48toDataView(view, offset, value);
    offset += 6;
  });
  return offset;
}

/**
 * Represents the index page of a PageStore. This page is updated in every commit and contains various things, e.g. the
 * last transaction id, binary patches for pages, a list of "new" pages etc..
 */
export class IndexPage extends MetaPageWithPatches {
  /** This exists to detect unexpected page size changes. */
  pageSize: number;

  /**
   * Transaction id of the (internal) page store used to store the transaction ids of all page group pages.
   */
  transactionIdsPageStoreTransactionId: number;

  readonly pageGroupNumberToTransactionId: Map<number, number> = new Map();

  constructor(bufferOrIndexPageOrUndefined: ArrayBuffer | IndexPage | undefined) {
    super();
    if (bufferOrIndexPageOrUndefined === undefined) {
      // default values for an empty page store

      this.pageSize = 0;

      this.transactionIdsPageStoreTransactionId = 0;
    } else if (bufferOrIndexPageOrUndefined instanceof IndexPage) {
      const sourceIndexPage = bufferOrIndexPageOrUndefined;
      // just copy all the data from sourceIndexPage
      this.transactionId = sourceIndexPage.transactionId;
      this.pageSize = sourceIndexPage.pageSize;
      this.transactionIdsPageStoreTransactionId = sourceIndexPage.transactionIdsPageStoreTransactionId;
      sourceIndexPage.pageNumberToPatches.forEach((patches, pageNumber) =>
        this.pageNumberToPatches.set(pageNumber, [...patches])
      );
      sourceIndexPage.pageGroupNumberToTransactionId.forEach((transactionIdForPage, pageGroupNumber) =>
        this.pageGroupNumberToTransactionId.set(pageGroupNumber, transactionIdForPage)
      );
    } else {
      const view = new DataView(bufferOrIndexPageOrUndefined);

      this.readHeader(view);

      let offset = MetaPageWithPatches.headerSerializedLength;

      this.pageSize = view.getUint32(offset);
      offset += 4;

      this.transactionIdsPageStoreTransactionId = readUint48FromDataView(view, offset);
      offset += 6;

      offset = this.readPatches(view, offset);

      offset = readUint32ToUint48Map(view, offset, this.pageGroupNumberToTransactionId);
    }
  }

  get serializedLength(): number {
    return (
      MetaPageWithPatches.headerSerializedLength +
      4 +
      6 +
      this.patchesSerializedLength +
      2 +
      10 * this.pageGroupNumberToTransactionId.size
    );
  }

  serialize(buffer: ArrayBuffer): void {
    const expectedLength = this.serializedLength;
    if (expectedLength > buffer.byteLength) {
      throw new Error("buffer is too small");
    }
    const view = new DataView(buffer);

    this.writeHeader(view);

    let offset = MetaPageWithPatches.headerSerializedLength;

    view.setUint32(offset, this.pageSize);
    offset += 4;

    writeUint48toDataView(view, offset, this.transactionIdsPageStoreTransactionId);
    offset += 6;

    offset = this.writePatches(view, offset);

    offset = writeUint32ToUint48Map(view, offset, this.pageGroupNumberToTransactionId);

    if (offset !== expectedLength) {
      throw new Error("expectedLength was wrong");
    }
  }

  /** Determine the page group number that contributes the most to the size of this index page. */
  determineLargestPageGroup(): number | undefined {
    const sizePerGroup = new Map<number, number>();

    this.pageNumberToPatches.forEach((patches, pageNumber) => {
      if (patches.length) {
        const groupNumber = pageNumberToPageGroupNumber(pageNumber);
        let size = sizePerGroup.get(groupNumber) ?? 0;
        size += 6;
        for (const patch of patches) {
          size += patch.serializedLength;
        }
        sizePerGroup.set(groupNumber, size);
      }
    });

    let largestGroupNumber: number | undefined = undefined;
    let largestSize: number | undefined = undefined;
    sizePerGroup.forEach((size, pageGroupNumber) => {
      if (largestSize === undefined || size > largestSize) {
        largestSize = size;
        largestGroupNumber = pageGroupNumber;
      }
    });
    return largestGroupNumber;
  }

  movePageGroupDataToPageGroup(pageGroupPage: PageGroupPage) {
    const pageGroupNumber = pageGroupPage.pageGroupNumber;

    this.pageNumberToPatches.forEach((patches, pageNumber) => {
      if (pageGroupNumber === pageNumberToPageGroupNumber(pageNumber)) {
        if (patches.length) {
          const mergedPatches = Patch.mergePatches([
            ...(pageGroupPage.pageNumberToPatches.get(pageNumber) ?? []),
            ...patches,
          ]);
          pageGroupPage.pageNumberToPatches.set(pageNumber, mergedPatches);
        }
        this.pageNumberToPatches.delete(pageNumber);
      }
    });
  }
}
