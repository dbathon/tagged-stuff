import { MetaPageWithPatches } from "./MetaPageWithPatches";
import { readUint48FromDataView, writeUint48toDataView } from "./util";

/**
 * Meta page that contains the latest transaction id and patches for 32 pages.
 */
export class PageGroupPage extends MetaPageWithPatches {
  /**
   * If a page in this page group has no transaction id then it only consists of patches in the index page and/or this
   * page group page or not at all.
   */
  readonly pageNumberToTransactionId: Map<number, number> = new Map();

  constructor(
    readonly pageNumberOffset: number,
    bufferOrPageGroupPageOrUndefined: ArrayBuffer | PageGroupPage | undefined
  ) {
    super();
    if (bufferOrPageGroupPageOrUndefined === undefined) {
      // nothing to do
    } else if (bufferOrPageGroupPageOrUndefined instanceof PageGroupPage) {
      const sourcePageGroupPage = bufferOrPageGroupPageOrUndefined;
      if (pageNumberOffset !== sourcePageGroupPage.pageNumberOffset) {
        throw new Error("pageNumberOffset does not match");
      }
      // just copy all the data from sourcePageGroupPage
      this.transactionId = sourcePageGroupPage.transactionId;
      sourcePageGroupPage.pageNumberToTransactionId.forEach((transactionIdForPage, pageNumber) =>
        this.pageNumberToTransactionId.set(pageNumber, transactionIdForPage)
      );
      sourcePageGroupPage.pageNumberToPatches.forEach((patches, pageNumber) =>
        this.pageNumberToPatches.set(pageNumber, [...patches])
      );
    } else {
      const view = new DataView(bufferOrPageGroupPageOrUndefined);

      this.readHeader(view);

      let offset = MetaPageWithPatches.headerSerializedLength;

      // read the 32 transaction ids
      for (let i = 0; i < 32; i++) {
        const transactionIdForPage = readUint48FromDataView(view, offset);
        offset += 6;
        if (transactionIdForPage > 0) {
          this.pageNumberToTransactionId.set(pageNumberOffset + i, transactionIdForPage);
        }
      }

      this.readPatches(view, offset);
    }
  }

  get serializedLength(): number {
    return MetaPageWithPatches.headerSerializedLength + 32 * 6 + this.patchesSerializedLength;
  }

  serialize(buffer: ArrayBuffer): void {
    const expectedLength = this.serializedLength;
    if (expectedLength > buffer.byteLength) {
      throw new Error("buffer is too small");
    }
    const view = new DataView(buffer);

    this.writeHeader(view);

    let offset = MetaPageWithPatches.headerSerializedLength;

    // write the 32 transaction ids
    for (let i = 0; i < 32; i++) {
      const transactionIdForPage = this.pageNumberToTransactionId.get(this.pageNumberOffset + i) ?? 0;
      writeUint48toDataView(view, offset, transactionIdForPage);
    }

    offset = this.writePatches(view, offset);

    if (offset !== expectedLength) {
      throw new Error("expectedLength was wrong");
    }
  }
}
