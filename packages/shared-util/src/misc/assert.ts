// TODO: remove message parameter entirely?!
export function assert(checkCondition: unknown, _errorMessage?: string | (() => string)): asserts checkCondition {
  if (!checkCondition) {
    throw new Error();
    // const message = typeof errorMessage === "string" ? errorMessage : errorMessage?.();
    // throw new Error(message);
  }
}
