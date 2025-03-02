export class Semaphore {
  private permitsTaken = 0;
  private queue: Array<() => void> = [];

  constructor(readonly permits: number) {
    if (permits < 1) {
      throw new Error("Semaphore must have at least one permit");
    }
  }

  private runNext(): void {
    if (this.permitsTaken < this.permits) {
      const next = this.queue.shift();
      if (next) {
        ++this.permitsTaken;
        next();
      }
    }
  }

  private acquire(): Promise<void> {
    return new Promise((resolve) => {
      this.queue.push(resolve);
      this.runNext();
    });
  }

  private release(): void {
    --this.permitsTaken;
    this.runNext();
  }

  async run<T>(task: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await task();
    } finally {
      this.release();
    }
  }

  get running(): number {
    return this.permitsTaken;
  }

  get queued(): number {
    return this.queue.length;
  }

  get runningOrQueued(): number {
    return this.running + this.queued;
  }
}
