import { Patch } from "./Patch";
import { readUint48FromDataView, writeUint48toDataView } from "./util";

/**
 * Represents the meta/main/index page of a PageStore. This page is updated in every commit and contains various
 * things, e.g. the last transaction id, binary patches for pages, a list of "new" pages etc..
 */
export class MetaPage {
  transactionId: number;

  maxPageNumber: number;

  /**
   * Pages that only consist of patches in the MetaPage.
   */
  readonly newPageNumbers: Set<number> = new Set();

  readonly pageNumberToPatches: Map<number, Patch[]> = new Map();

  readonly pageNumberToTransactionId: Map<number, number> = new Map();

  constructor(bufferOrMetaPageOrUndefined: ArrayBuffer | MetaPage | undefined) {
    if (bufferOrMetaPageOrUndefined === undefined) {
      // default values for an empty page store

      // start with transactionId 0, the first one that is actually committed will be 1
      this.transactionId = 0;

      // no pages yet
      this.maxPageNumber = -1;
    } else if (bufferOrMetaPageOrUndefined instanceof MetaPage) {
      const sourceMetaPage = bufferOrMetaPageOrUndefined;
      // just copy all the data from sourceMetaPage
      this.transactionId = sourceMetaPage.transactionId;
      this.maxPageNumber = sourceMetaPage.maxPageNumber;
      sourceMetaPage.newPageNumbers.forEach((pageNumber) => this.newPageNumbers.add(pageNumber));
      sourceMetaPage.pageNumberToPatches.forEach((patches, pageNumber) =>
        this.pageNumberToPatches.set(pageNumber, [...patches])
      );
      sourceMetaPage.pageNumberToTransactionId.forEach((transactionIdForPage, pageNumber) =>
        this.pageNumberToTransactionId.set(pageNumber, transactionIdForPage)
      );
    } else {
      const view = new DataView(bufferOrMetaPageOrUndefined);
      // check
      // the first two bytes are the "format version", for now it just needs to be 1
      const version = view.getUint16(0);
      if (version !== 1) {
        throw new Error("unexpected version: " + version);
      }

      this.transactionId = readUint48FromDataView(view, 2);

      this.maxPageNumber = view.getUint32(8);

      let offset = 12;

      const countNewPageNumbers = view.getUint16(offset);
      offset += 2;
      for (let i = 0; i < countNewPageNumbers; i++) {
        this.newPageNumbers.add(view.getUint32(offset));
        offset += 4;
      }

      const countPageNumbersWithPatches = view.getUint16(offset);
      offset += 2;
      for (let i = 0; i < countPageNumbersWithPatches; i++) {
        const pageNumber = view.getUint32(offset);
        offset += 4;
        const countPatches = view.getUint16(offset);
        offset += 2;
        const patches: Patch[] = [];
        for (let j = 0; j < countPatches; j++) {
          const patch = Patch.deserialize(view, offset);
          offset += patch.serializedLength;
          patches.push(patch);
        }
        this.pageNumberToPatches.set(pageNumber, patches);
      }

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
    let patchesSize = 0;
    this.pageNumberToPatches.forEach((patches) => {
      if (patches.length) {
        patchesSize += 6;
        for (const patch of patches) {
          patchesSize += patch.serializedLength;
        }
      }
    });

    return 12 + 2 + 4 * this.newPageNumbers.size + 2 + patchesSize + 2 + 10 * this.pageNumberToPatches.size;
  }

  serialize(buffer: ArrayBuffer): void {
    if (this.serializedLength > buffer.byteLength) {
      throw new Error("buffer is too small");
    }
    const view = new DataView(buffer);

    // version
    view.setUint16(0, 1);

    writeUint48toDataView(view, 2, this.transactionId);

    view.setUint32(8, this.maxPageNumber);

    let offset = 12;

    view.setUint16(offset, this.newPageNumbers.size);
    offset += 2;
    this.newPageNumbers.forEach((pageNumber) => {
      view.setUint32(offset, pageNumber);
      offset += 4;
    });

    let countPageNumbersWithPatches = 0;
    const offsetForCountPageNumbersWithPatches = offset;
    offset += 2;
    this.pageNumberToPatches.forEach((patches, pageNumber) => {
      if (patches.length) {
        countPageNumbersWithPatches += 1;
        view.setUint32(offset, pageNumber);
        offset += 4;
        view.setUint16(offset, patches.length);
        offset += 2;
        for (const patch of patches) {
          patch.serialize(view, offset);
          offset += patch.serializedLength;
        }
      }
    });
    view.setUint16(offsetForCountPageNumbersWithPatches, countPageNumbersWithPatches);

    view.setUint16(offset, this.pageNumberToTransactionId.size);
    offset += 2;
    this.pageNumberToTransactionId.forEach((transactionIdForPage, pageNumber) => {
      view.setUint32(offset, pageNumber);
      offset += 4;
      writeUint48toDataView(view, offset, transactionIdForPage);
      offset += 6;
    });
  }
}
