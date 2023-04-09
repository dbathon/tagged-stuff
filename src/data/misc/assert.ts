export function assert(checkCondition: unknown, errorMessage?: string | (() => string)): asserts checkCondition {
  if (!checkCondition) {
    const message = typeof errorMessage === "string" ? errorMessage : errorMessage?.();
    throw new Error(message);
  }
}
