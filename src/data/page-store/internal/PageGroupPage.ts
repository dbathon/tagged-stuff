import { MetaPageWithPatches } from "./MetaPageWithPatches";
import { readUint48FromDataView, writeUint48toDataView } from "./util";

export const PAGES_PER_PAGE_GROUP = 32;

export function pageNumberToPageGroupNumber(pageNumber: number) {
  // we can't use shifting for this, since the page number is uint32
  return Math.floor(pageNumber / PAGES_PER_PAGE_GROUP);
}

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
    readonly pageGroupNumber: number,
    bufferOrPageGroupPageOrUndefined: ArrayBuffer | PageGroupPage | undefined
  ) {
    super();
    if (bufferOrPageGroupPageOrUndefined === undefined) {
      // nothing to do
    } else if (bufferOrPageGroupPageOrUndefined instanceof PageGroupPage) {
      const sourcePageGroupPage = bufferOrPageGroupPageOrUndefined;
      if (pageGroupNumber !== sourcePageGroupPage.pageGroupNumber) {
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
      const pageNumberOffset = pageGroupNumber * PAGES_PER_PAGE_GROUP;
      for (let i = 0; i < PAGES_PER_PAGE_GROUP; i++) {
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
    return MetaPageWithPatches.headerSerializedLength + PAGES_PER_PAGE_GROUP * 6 + this.patchesSerializedLength;
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
    const pageNumberOffset = this.pageGroupNumber * PAGES_PER_PAGE_GROUP;
    for (let i = 0; i < PAGES_PER_PAGE_GROUP; i++) {
      const transactionIdForPage = this.pageNumberToTransactionId.get(pageNumberOffset + i) ?? 0;
      writeUint48toDataView(view, offset, transactionIdForPage);
      offset += 6;
    }

    offset = this.writePatches(view, offset);

    if (offset !== expectedLength) {
      throw new Error("expectedLength was wrong");
    }
  }

  /** Determine the page number that contributes the most to the size of this page group page. */
  determineLargestPage(): number | undefined {
    let largestPageNumber: number | undefined = undefined;
    let largestSize: number | undefined = undefined;
    this.pageNumberToPatches.forEach((patches, pageNumber) => {
      if (patches.length) {
        let size = 0;
        for (const patch of patches) {
          size += patch.serializedLength;
        }
        if (largestSize === undefined || size > largestSize) {
          largestSize = size;
          largestPageNumber = pageNumber;
        }
      }
    });

    return largestPageNumber;
  }
}
