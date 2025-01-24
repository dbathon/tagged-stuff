/**
 * Defines the order of "JSON values" for sorting.
 *
 * This needs to be kept in sync with JsonEventType order.
 */
export function compareJsonPrimitives(a: unknown, b: unknown): -1 | 0 | 1 {
  if (typeof a === "number") {
    if (typeof b === "number") {
      if (a < b) {
        return -1;
      } else {
        return a === b ? 0 : 1;
      }
    } else {
      // number is before everything else
      return -1;
    }
  } else if (typeof a === "string") {
    if (typeof b === "string") {
      if (a < b) {
        return -1;
      } else {
        return a === b ? 0 : 1;
      }
    } else if (typeof b === "number") {
      return 1;
    } else {
      return -1;
    }
  } else if (typeof a === "boolean") {
    if (typeof b === "boolean") {
      if (a < b) {
        return -1;
      } else {
        return a === b ? 0 : 1;
      }
    } else if (typeof b === "number" || typeof b === "string") {
      return 1;
    } else {
      return -1;
    }
  } else if (a === null) {
    if (b === null) {
      return 0;
    } else if (typeof b === "number" || typeof b === "string" || typeof b === "boolean") {
      return 1;
    } else {
      return -1;
    }
  } else {
    if (typeof b === "number" || typeof b === "string" || typeof b === "boolean" || b === null) {
      // everything that is not a "JSON primitive" is considered to be after the primitives
      return 1;
    } else {
      // everything else is considered equal to everything else
      return 0;
    }
  }
}
