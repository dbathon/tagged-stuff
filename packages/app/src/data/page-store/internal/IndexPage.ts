import { uint8ArrayToDataView } from "shared-util";
import { Patch } from "./Patch";
import { readUint48FromDataView, writeUint48toDataView } from "./util";

/**
 * Represents the index page of a PageStore. This page is updated in every commit and contains basic things like the
 * page size of this page store and the the transaction id of the root page of the transaction tree and then mainly
 * binary patches for pages.
 */
export class IndexPage {
  constructor(
    /**
     * The transaction id of the index page, not actually stored in the page itself, but kept as a property in this
     * class for convenience.
     */
    readonly transactionId: number,
    /** This exists to detect unexpected page size changes. */
    readonly pageSize: number,
    /**
     * The transaction ids of all stored pages are stored in a "static" tree (see TreeCalc) where the parent pages
     * contain the transaction ids of the child pages, only the transaction id of the root needs to be stored in the
     * index page.
     */
    readonly transactionTreeRootTransactionId: number,
    readonly pageNumberToPatches: Map<number, Patch[]>,
  ) {}

  static deserialize(transactionId: number, pageData: Uint8Array, expectedPageSize: number): IndexPage {
    if (pageData.length === 0) {
      // special case, initial state
      return new IndexPage(0, expectedPageSize, 0, new Map());
    }
    const view = uint8ArrayToDataView(pageData);
    let offset = 0;

    // the first two bytes are the "format version", for now it just needs to be 1
    const version = view.getUint16(offset);
    if (version !== 1) {
      throw new Error("unexpected version: " + version);
    }
    offset += 2;

    const pageSize = view.getUint32(offset);
    if (pageSize !== expectedPageSize) {
      throw new Error("unexpected pageSize: " + pageSize + ", expected: " + expectedPageSize);
    }
    offset += 4;

    const transactionTreeRootTransactionId = readUint48FromDataView(view, offset);
    offset += 6;

    const pageNumberToPatches: Map<number, Patch[]> = new Map();

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
      pageNumberToPatches.set(pageNumber, patches);
    }

    if (offset > pageData.length) {
      throw new Error("pageData incomplete");
    }

    return new IndexPage(transactionId, pageSize, transactionTreeRootTransactionId, pageNumberToPatches);
  }

  private get patchesSerializedLength(): number {
    let patchesSize = 2;
    this.pageNumberToPatches.forEach((patches) => {
      if (patches.length) {
        patchesSize += 6;
        for (const patch of patches) {
          patchesSize += patch.serializedLength;
        }
      }
    });
    return patchesSize;
  }

  get serializedLength(): number {
    return (
      2 + // version
      4 + // pageSize
      6 + // transactionTreeRootTransactionId
      this.patchesSerializedLength
    );
  }

  private serializePatches(view: DataView, startOffset: number): number {
    let offset = startOffset;
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
    return offset;
  }

  serialize(): Uint8Array {
    const expectedLength = this.serializedLength;
    const array: Uint8Array = new Uint8Array(expectedLength);
    if (expectedLength > array.byteLength) {
      throw new Error("buffer is too small");
    }
    const view = uint8ArrayToDataView(array);

    let offset = 0;
    // version
    view.setUint16(offset, 1);
    offset += 2;

    view.setUint32(offset, this.pageSize);
    offset += 4;

    writeUint48toDataView(view, offset, this.transactionTreeRootTransactionId);
    offset += 6;

    offset = this.serializePatches(view, offset);

    if (offset !== expectedLength) {
      throw new Error("expectedLength was wrong");
    }
    return array;
  }
}
