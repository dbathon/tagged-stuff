export interface TreePathElement {
  readonly pageNumber: number;
  /** The offset of the entry on the page. */
  readonly offset: number;
}

/**
 * TODO...
 * TODO maybe optimize maxPageNumber to avoid wasting pages?
 */
export class TreeCalc {
  /** The first page number for each level (from the bottom up). */
  private readonly levelStarts: number[];
  /** The maximum page number that can be used for normal pages. */
  readonly maxPageNumber: number;
  readonly divider: number;

  constructor(readonly pageSize: number, readonly entrySize: number, maxPageNumber: number) {
    const levelStarts: number[] = [];
    const divider = Math.floor(pageSize / entrySize);
    let levelDivider = divider;
    let pageNumberCeil = maxPageNumber;
    // build the levels from the bottom up
    while (true) {
      const pageCount = Math.ceil(maxPageNumber / levelDivider);
      levelStarts.push(pageNumberCeil - (pageCount - 1));

      if (pageCount <= 1) {
        break;
      }
      levelDivider *= divider;
      pageNumberCeil -= pageCount;
    }

    this.levelStarts = levelStarts;
    this.maxPageNumber = levelStarts[levelStarts.length - 1] - 1;
    this.divider = divider;
  }

  get height(): number {
    return this.levelStarts.length;
  }

  getPath(pageNumber: number): TreePathElement[] {
    if (pageNumber > this.maxPageNumber) {
      throw new Error("pageNumber is too large");
    }
    const result: TreePathElement[] = [];
    let index = pageNumber;
    for (const levelStart of this.levelStarts) {
      const nextIndex = Math.floor(index / this.divider);
      result.unshift({
        pageNumber: levelStart + nextIndex,
        offset: (index % this.divider) * this.entrySize,
      });
      index = nextIndex;
    }
    return result;
  }
}
