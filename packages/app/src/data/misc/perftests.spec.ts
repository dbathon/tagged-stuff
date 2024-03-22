import { test } from "vitest";

const n = 1; //10000000;

function t1_empty() {}

function t1() {
  for (let i = 0; i < n; i++) {
    t1_empty();
  }
}

function t2_single(): number {
  return 1;
}

function t2() {
  // @ts-ignore
  let sum = 0;
  for (let i = 0; i < n; i++) {
    const v = t2_single();
    sum += v + v;
  }
}

function t3_array(): [number, number] {
  return [1, 1];
}

function t3() {
  // @ts-ignore
  let sum = 0;
  for (let i = 0; i < n; i++) {
    const v = t3_array();
    sum += v[0] + v[1];
  }
}

function t3_at() {
  // @ts-ignore
  let sum = 0;
  for (let i = 0; i < n; i++) {
    const v = t3_array();
    sum += v.at(0)! + v.at(1)!;
  }
}

function t3_destructure() {
  // @ts-ignore
  let sum = 0;
  for (let i = 0; i < n; i++) {
    const [a, b] = t3_array();
    sum += a + b;
  }
}

function t4_object(): { a: number; b: number } {
  return { a: 1, b: 1 };
}

function t4() {
  // @ts-ignore
  let sum = 0;
  for (let i = 0; i < n; i++) {
    const v = t4_object();
    sum += v.a + v.b;
  }
}

function t4_destructure() {
  // @ts-ignore
  let sum = 0;
  for (let i = 0; i < n; i++) {
    const { a, b } = t4_object();
    sum += a + b;
  }
}

function testFn(fn: () => unknown) {
  // const start = Date.now();
  fn();
  // console.log(fn.name + " took " + (Date.now() - start) + " ms");
}

function testAll() {
  testFn(t1);
  testFn(t2);
  testFn(t3);
  testFn(t3_at);
  testFn(t3_destructure);
  testFn(t4);
  testFn(t4_destructure);
}

test("perf", () => {
  testAll();
});
