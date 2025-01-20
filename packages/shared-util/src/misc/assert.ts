let messagesEnabled = false;

export function assert(checkCondition: unknown, errorMessage?: string | (() => string)): asserts checkCondition {
  if (!checkCondition) {
    if (messagesEnabled) {
      const message = typeof errorMessage === "string" ? errorMessage : errorMessage?.();
      throw new Error(message);
    } else {
      throw new Error();
    }
  }
}

/**
 * This can be called in "dev builds" to "enable" the errorMessage parameter for assert(). If it is not called, then it
 * is possible to omit all the messages from the build via tree shaking.
 */
export function enableMessagesForAssert() {
  messagesEnabled = true;
}
