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
