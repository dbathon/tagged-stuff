export function jsonEquals(a: unknown, b: unknown): boolean {
  if (a === b) {
    return true;
  }
  if (typeof a === "object" && typeof b === "object") {
    if (Array.isArray(b)) {
      if (Array.isArray(a)) {
        const length = a.length;
        if (b.length !== length) {
          return false;
        }

        for (let i = 0; i < length; i++) {
          if (!jsonEquals(a[i], b[i])) {
            return false;
          }
        }
        return true;
      }
    } else if (a && b && !Array.isArray(a)) {
      // both a and b are objects
      const allKeys = new Set([...Object.keys(a), ...Object.keys(b)]);
      for (const key of allKeys) {
        if (!jsonEquals((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key])) {
          return false;
        }
      }
      return true;
    }
  }

  return false;
}
