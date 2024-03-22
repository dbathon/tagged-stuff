import { Patch } from "./Patch";
import { readUint48FromDataView, writeUint48toDataView } from "./util";

/**
 * Contains shared functionality between IndexPage and PageGroupPage.
 */
export abstract class MetaPageWithPatches {
  /** Start with transactionId 0, the first one that is actually committed will be larger than 0. */
  transactionId: number = 0;

  readonly pageNumberToPatches: Map<number, Patch[]> = new Map();

  protected static readonly headerSerializedLength = 8;

  protected readHeader(view: DataView): void {
    // the first two bytes are the "format version", for now it just needs to be 1
    const version = view.getUint16(0);
    if (version !== 1) {
      throw new Error("unexpected version: " + version);
    }

    this.transactionId = readUint48FromDataView(view, 2);
  }

  protected readPatches(view: DataView, startOffset: number): number {
    let offset = startOffset;
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
    return offset;
  }

  protected get patchesSerializedLength(): number {
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

  protected writeHeader(view: DataView): void {
    // version
    view.setUint16(0, 1);

    writeUint48toDataView(view, 2, this.transactionId);
  }

  protected writePatches(view: DataView, startOffset: number): number {
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
}
