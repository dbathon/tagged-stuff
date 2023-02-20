import { PageData } from "../PageData";

export function readUint48FromDataView(view: DataView, offset: number): number {
  const highPart = view.getUint16(offset);
  if (highPart !== 0) {
    // TODO actually implement 48 bit numbers
    throw new Error("large numbers are not supported yet");
  }
  return view.getUint32(offset + 2);
}

export function writeUint48toDataView(view: DataView, offset: number, value: number): void {
  if (value >= (1 << 30) * 4) {
    // TODO actually implement 48 bit numbers
    throw new Error("large numbers are not supported yet");
  }
  view.setUint16(offset, 0);
  view.setUint32(offset + 2, value);
}

export function dataViewsEqual(a: DataView, b: DataView): boolean {
  if (a === b) {
    return true;
  }
  const length = a.byteLength;
  if (length !== b.byteLength) {
    return false;
  }
  for (let i = 0; i < length; i += 4) {
    if (i + 4 <= length) {
      // common/fast case
      if (a.getUint32(i) !== b.getUint32(i)) {
        return false;
      }
    } else {
      // check the last bytes
      while (i < length) {
        if (a.getUint8(i) !== b.getUint8(i)) {
          return false;
        }
        i++;
      }
    }
  }
  return true;
}

export function copyPageData(pageData: PageData): PageData {
  const result = new PageData(new ArrayBuffer(pageData.buffer.byteLength));
  result.array.set(pageData.array);
  return result;
}
