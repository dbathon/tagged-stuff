class QueuedOperation {
  private completionPromise?: Promise<void>;
  private completionPromiseResolve?: () => void;

  constructor(
    readonly type: "R" | "W",
    readonly previousCompleted?: Promise<void>,
  ) {}

  get afterCompletion(): Promise<void> {
    if (!this.completionPromise) {
      this.completionPromise = new Promise((resolve) => {
        this.completionPromiseResolve = resolve;
      });
    }
    return this.completionPromise;
  }

  complete() {
    if (this.completionPromiseResolve) {
      this.completionPromiseResolve();
    }
  }
}

export class ReadWriteLock {
  private lastQueuedOperation?: QueuedOperation;

  private reads = 0;
  private write = false;

  private queueIfNecessary(operationType: "R" | "W"): QueuedOperation {
    if (!this.lastQueuedOperation) {
      this.lastQueuedOperation = new QueuedOperation(operationType);
    } else if (operationType === "R" && this.lastQueuedOperation.type === "R") {
      // nothing to do, the reads can happen in parallel
    } else {
      this.lastQueuedOperation = new QueuedOperation(operationType, this.lastQueuedOperation.afterCompletion);
    }
    return this.lastQueuedOperation;
  }

  private completeOperation(queuedOperation: QueuedOperation) {
    queuedOperation.complete();
    if (queuedOperation === this.lastQueuedOperation) {
      this.lastQueuedOperation = undefined;
    }
  }

  async withReadLock<T>(action: () => Promise<T>): Promise<T> {
    const queuedOperation = this.queueIfNecessary("R");
    if (queuedOperation.previousCompleted) {
      await queuedOperation.previousCompleted;
    }
    if (this.write || this.reads < 0) {
      // sanity check
      throw new Error("invalid state for read: " + this.write + ", " + this.reads);
    }
    // multiple reads can be active at the same time
    ++this.reads;
    try {
      return await action();
    } finally {
      --this.reads;
      if (this.reads === 0) {
        this.completeOperation(queuedOperation);
      }
    }
  }

  async withWriteLock<T>(action: () => Promise<T>): Promise<T> {
    const queuedOperation = this.queueIfNecessary("W");
    if (queuedOperation.previousCompleted) {
      await queuedOperation.previousCompleted;
    }
    if (this.write || this.reads !== 0) {
      // sanity check
      throw new Error("invalid state for write: " + this.write + ", " + this.reads);
    }
    this.write = true;
    try {
      return await action();
    } finally {
      this.write = false;
      this.completeOperation(queuedOperation);
    }
  }
}
