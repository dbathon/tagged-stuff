import { MetaPageWithPatches } from "./MetaPageWithPatches";
import { readUint48FromDataView, writeUint48toDataView } from "./util";

/**
 * Represents the index page of a PageStore. This page is updated in every commit and contains various things, e.g. the
 * last transaction id, binary patches for pages, a list of "new" pages etc..
 */
export class IndexPage extends MetaPageWithPatches {
  maxPageNumber: number;

  /**
   * Pages that only consist of patches in the IndexPage.
   */
  readonly newPageNumbers: Set<number> = new Set();

  readonly pageNumberToTransactionId: Map<number, number> = new Map();

  constructor(bufferOrIndexPageOrUndefined: ArrayBuffer | IndexPage | undefined) {
    super();
    if (bufferOrIndexPageOrUndefined === undefined) {
      // default values for an empty page store

      // start with transactionId 0, the first one that is actually committed will be 1
      this.transactionId = 0;

      // no pages yet
      this.maxPageNumber = -1;
    } else if (bufferOrIndexPageOrUndefined instanceof IndexPage) {
      const sourceIndexPage = bufferOrIndexPageOrUndefined;
      // just copy all the data from sourceIndexPage
      this.transactionId = sourceIndexPage.transactionId;
      this.maxPageNumber = sourceIndexPage.maxPageNumber;
      sourceIndexPage.newPageNumbers.forEach((pageNumber) => this.newPageNumbers.add(pageNumber));
      sourceIndexPage.pageNumberToPatches.forEach((patches, pageNumber) =>
        this.pageNumberToPatches.set(pageNumber, [...patches])
      );
      sourceIndexPage.pageNumberToTransactionId.forEach((transactionIdForPage, pageNumber) =>
        this.pageNumberToTransactionId.set(pageNumber, transactionIdForPage)
      );
    } else {
      const view = new DataView(bufferOrIndexPageOrUndefined);

      this.readHeader(view);

      let offset = MetaPageWithPatches.headerSerializedLength;

      this.maxPageNumber = view.getUint32(8);
      offset += 4;

      const countNewPageNumbers = view.getUint16(offset);
      offset += 2;
      for (let i = 0; i < countNewPageNumbers; i++) {
        this.newPageNumbers.add(view.getUint32(offset));
        offset += 4;
      }

      offset = this.readPatches(view, offset);

      const countPageNumberToTransactionId = view.getUint16(offset);
      offset += 2;
      for (let i = 0; i < countPageNumberToTransactionId; i++) {
        const pageNumber = view.getUint32(offset);
        offset += 4;
        const transactionIdForPage = readUint48FromDataView(view, offset);
        offset += 6;
        this.pageNumberToTransactionId.set(pageNumber, transactionIdForPage);
      }
    }
  }

  get serializedLength(): number {
    return (
      MetaPageWithPatches.headerSerializedLength +
      4 +
      2 +
      4 * this.newPageNumbers.size +
      this.patchesSerializedLength +
      2 +
      10 * this.pageNumberToPatches.size
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

    view.setUint32(8, this.maxPageNumber);
    offset += 4;

    view.setUint16(offset, this.newPageNumbers.size);
    offset += 2;
    this.newPageNumbers.forEach((pageNumber) => {
      view.setUint32(offset, pageNumber);
      offset += 4;
    });

    offset = this.writePatches(view, offset);

    view.setUint16(offset, this.pageNumberToTransactionId.size);
    offset += 2;
    this.pageNumberToTransactionId.forEach((transactionIdForPage, pageNumber) => {
      view.setUint32(offset, pageNumber);
      offset += 4;
      writeUint48toDataView(view, offset, transactionIdForPage);
      offset += 6;
    });

    if (offset !== expectedLength) {
      throw new Error("expectedLength was wrong");
    }
  }
}
