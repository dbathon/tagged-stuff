export function assert(checkCondition: unknown, errorMessage?: string | (() => string)): asserts checkCondition {
  if (!checkCondition) {
    if (import.meta.env.PROD) {
      throw new Error();
    } else {
      const message = typeof errorMessage === "string" ? errorMessage : errorMessage?.();
      throw new Error(message);
    }
  }
}
