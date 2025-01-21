import { assert } from "shared-util";

export type TransactionIdLocation = {
  readonly pageNumber: number;
  /** The offset of the entry on the page. */
  readonly offset: number;
};

function determineLevelStarts(entriesPerPage: number, maxPageNumber: number, maxNormalPageNumber: number): number[] {
  const levelStarts: number[] = [];
  let pageNumberCeil = maxPageNumber;
  let entriesInLevel = maxNormalPageNumber;
  // build the levels from the bottom up
  while (true) {
    const pageCount = Math.ceil(entriesInLevel / entriesPerPage);
    levelStarts.push(pageNumberCeil - (pageCount - 1));

    if (pageCount <= 1) {
      break;
    }
    entriesInLevel = pageCount;
    pageNumberCeil -= pageCount;
  }

  return levelStarts;
}

/**
 * TODO...
 */
export class TreeCalc {
  /** The first page number for each level (from the bottom up). */
  private readonly levelStarts: number[];
  /** The maximum page number that can be used for normal pages. */
  readonly maxNormalPageNumber: number;
  readonly entriesPerPage: number;

  constructor(
    readonly pageSize: number,
    readonly entrySize: number,
    readonly maxPageNumber: number,
  ) {
    const entriesPerPage = Math.floor(pageSize / entrySize);

    let levelStarts: number[];
    let guessedMaxNormalPageNumber = maxPageNumber;
    let maxNormalPageNumber: number;

    // determine the optimal maxNormalPageNumber (this should converge very fast)
    while (true) {
      levelStarts = determineLevelStarts(entriesPerPage, maxPageNumber, guessedMaxNormalPageNumber);
      maxNormalPageNumber = levelStarts[levelStarts.length - 1] - 1;
      if (maxNormalPageNumber === guessedMaxNormalPageNumber) {
        break;
      }
      guessedMaxNormalPageNumber = maxNormalPageNumber;
    }

    this.levelStarts = levelStarts;
    this.maxNormalPageNumber = maxNormalPageNumber;
    this.entriesPerPage = entriesPerPage;
  }

  get height(): number {
    return this.levelStarts.length;
  }

  getTransactionIdLocation(pageNumber: number): TransactionIdLocation | undefined {
    if (pageNumber < 0 || pageNumber > this.maxPageNumber) {
      throw new Error("invalid pageNumber: " + pageNumber);
    }
    let index = pageNumber <= this.maxNormalPageNumber ? pageNumber : undefined;
    for (const levelStart of this.levelStarts) {
      if (index === undefined) {
        if (pageNumber >= levelStart) {
          // we can now determine the index for the next level up
          index = pageNumber - levelStart;
        }
        continue;
      }
      return {
        pageNumber: levelStart + Math.floor(index / this.entriesPerPage),
        offset: (index % this.entriesPerPage) * this.entrySize,
      };
    }
    // it must be the root page number
    assert(pageNumber === this.maxNormalPageNumber + 1);
    return undefined;
  }
}
