/**
 * A simple immutable wrapper around an ArrayBuffer that also provides lazily constructed DataView and UInt8Array
 * views.
 */
export class PageData {
  private _dataView?: DataView;
  private _array?: Uint8Array;

  constructor(readonly buffer: ArrayBuffer) {}

  get dataView(): DataView {
    let result = this._dataView;
    if (!result) {
      result = this._dataView = new DataView(this.buffer);
    }
    return result;
  }

  get array(): Uint8Array {
    let result = this._array;
    if (!result) {
      result = this._array = new Uint8Array(this.buffer);
    }
    return result;
  }
}
