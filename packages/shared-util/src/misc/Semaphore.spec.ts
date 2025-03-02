import { assert, expect, test } from "vitest";
import { Semaphore } from "./Semaphore";

async function runMicrotasks() {
  await new Promise((resolve) => setTimeout(resolve));
}

async function testSemaphore(permits: number) {
  const semaphore = new Semaphore(permits);

  expect(semaphore.runningOrQueued).toBe(0);

  const taskCount = permits + 4;
  const blockerResolvers: (() => void)[] = [];
  const tasks: (() => Promise<void>)[] = [];
  const states: number[] = [];

  for (let i = 0; i < taskCount; i++) {
    const current = i;
    states[current] = 0;
    const blocker = new Promise<void>((resolve) => (blockerResolvers[current] = resolve));
    tasks[current] = async () => {
      states[current] = 1;
      await blocker;
      states[current] = 2;
    };
  }

  const taskPromises: Promise<void>[] = [];

  for (let i = 0; i < permits; i++) {
    taskPromises[i] = semaphore.run(tasks[i]);

    expect(semaphore.running).toBe(i + 1);
    expect(semaphore.queued).toBe(0);
    expect(semaphore.runningOrQueued).toBe(i + 1);
  }
  for (let i = permits; i < taskCount; i++) {
    taskPromises[i] = semaphore.run(tasks[i]);

    expect(semaphore.running).toBe(permits);
    expect(semaphore.queued).toBe(i - permits + 1);
    expect(semaphore.runningOrQueued).toBe(i + 1);
  }

  for (let i = 0; i < taskCount; i++) {
    await runMicrotasks();

    for (let j = 0; j < taskCount; j++) {
      expect(states[j]).toBe(j < i ? 2 : j < i + permits ? 1 : 0);
    }
    expect(semaphore.running).toBeLessThanOrEqual(permits);
    expect(semaphore.queued).toBe(Math.max(taskCount - permits - i, 0));

    blockerResolvers[i]();
  }

  await Promise.all(taskPromises);

  for (let i = 0; i < taskCount; i++) {
    expect(states[i]).toBe(2);
  }
  expect(semaphore.running).toBe(0);
  expect(semaphore.queued).toBe(0);
}

test("Semaphore", async () => {
  await testSemaphore(1);
  await testSemaphore(2);
  await testSemaphore(10);
});
