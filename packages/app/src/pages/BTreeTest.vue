<script setup lang="ts">
import type { BTreeNode } from "../shared/remote-b-tree";
import { BTreeSet } from "../shared/b-tree-set";
import { ref } from "vue";

async function testBTree() {
  const nodes: BTreeNode[] = [
    {
      id: "root",
      keys: ["h"],
      children: {
        ids: ["c1", "c2"],
        keyCounts: [3, 3],
      },
    },
    {
      id: "c1",
      keys: ["b", "d", "f"],
    },
    {
      id: "c2",
      keys: ["m", "n", "o"],
    },
  ];
  const tree = new BTreeSet(500);
  tree.data.clear();
  nodes.forEach((node) => tree.data.set(node.id, node));
  tree.rootId = "root";

  const chars = "abcdefghijklmnopqrstuvwxyz".split("");
  for (const key of chars) {
    console.log(key, await tree.contains(key).toPromise());
  }

  tree.clear();

  for (const key of [...chars].reverse()) {
    await tree.insert(key).toPromise();
  }
  for (const key of chars) {
    await tree.insert(key).toPromise();
  }
  console.log((await tree.dumpTree()).join("\n"));

  for (const key of chars) {
    console.log(key, await tree.contains(key).toPromise());
  }

  tree.clear();
  const testSize = 2000;
  for (let i = 0; i < testSize; ++i) {
    await tree.insert("" + i).toPromise();
  }
  console.log((await tree.dumpTree()).join("\n"));

  console.log(await tree.simpleScan(20, "2").toPromise());
  if ((await tree.simpleScan().toPromise()).length != testSize || (await tree.getKeyCount().toPromise()) != testSize) {
    throw new Error("scan failed");
  }

  for (let i = 0; i < testSize; ++i) {
    const key = "" + i;
    const result = await tree.simpleScan(1, key).toPromise();
    if (!(result.length === 1 && result[0] === key)) {
      throw new Error("scan failed");
    }
  }
}

async function bTreeBenchmark() {
  for (const entryCount of [100, 500, 1000, 5000, 30000]) {
    console.log("start", entryCount);
    for (const maxNodeSize of [30, 50, 100, 300, 1000, 5000]) {
      const start = new Date().getTime();
      const tree: BTreeSet = new BTreeSet(maxNodeSize);
      for (let i = 0; i < entryCount; ++i) {
        const str = "" + i;
        await tree.insert(str).toPromise();
      }
      if ((await tree.getKeyCount().toPromise()) !== entryCount) {
        throw new Error();
      }
      for (let i = entryCount - 1; i >= 0; --i) {
        const str = "" + i;
        await tree.delete(str).toPromise();
      }
      if ((await tree.getKeyCount().toPromise()) !== 0) {
        throw new Error();
      }

      const end = new Date().getTime();
      console.log(end - start, maxNodeSize, tree.rootId);
    }
  }
}

async function bTreeBenchmark2() {
  const testSize = 5000;
  const testMaxNodeSize = 1000;
  for (const withPromise of [false, true]) {
    for (let j = 0; j < 4; ++j) {
      console.log("bTreeBenchmark2", withPromise, j);
      let start = new Date().getTime();
      const tree: BTreeSet = new BTreeSet(testMaxNodeSize);
      tree.fetchNodeWithPromise = withPromise;
      const entryCount = testSize;
      for (let i = 0; i < entryCount; ++i) {
        const str = "" + i;
        const setResult = tree.insert(str);
        if (!setResult.hasValue) {
          await setResult.promise;
        }
      }
      console.log("- insert done", new Date().getTime() - start);

      if ((await tree.getKeyCount().toPromise()) !== entryCount) {
        throw new Error();
      }

      start = new Date().getTime();
      let result = false;
      try {
        for (let i = 0; i < entryCount; ++i) {
          const str = "" + i;
          const containsResult = tree.contains(str);
          if (containsResult.hasValue) {
            result = containsResult.value;
          } else {
            result = await containsResult.promise;
          }
        }
      } catch (e) {
        console.log(e);
      }
      console.log("- get done", new Date().getTime() - start, result);

      start = new Date().getTime();
      for (let i = entryCount - 1; i >= 0; --i) {
        const str = "" + i;
        const deleteResult = tree.delete(str);
        if (!deleteResult.hasValue) {
          await deleteResult.promise;
        }
      }
      console.log("- delete done", new Date().getTime() - start);

      if ((await tree.getKeyCount().toPromise()) !== 0) {
        throw new Error();
      }
    }
  }
}

let treeElement = ref("");
let treeMaxNodeSize = ref("30");

let tree: BTreeSet = new BTreeSet(30);
let treeDump = ref("");

async function updateTreeDump() {
  const entries = await tree.simpleScan().toPromise();
  treeDump.value =
    "size: " +
    entries.length +
    ", " +
    (await tree.getKeyCount().toPromise()) +
    "\nmaxNodeSize: " +
    tree.tree.maxNodeSize +
    "\nentries: " +
    entries.join(", ") +
    "\nnodesMap size: " +
    tree.data.size +
    "\n\n" +
    (await tree.dumpTree()).join("\n");
}

async function treeInit() {
  tree = new BTreeSet(parseInt(treeMaxNodeSize.value));
  await updateTreeDump();
}

async function treeInsert() {
  await tree.insert(treeElement.value).toPromise();
  treeElement.value = "" + Math.floor(Math.random() * 10000);
  await updateTreeDump();
}

async function treeInsertRandom() {
  for (let i = 0; i < 10; ++i) {
    const element: string = "" + Math.floor(Math.random() * 10000);
    await tree.insert(element).toPromise();
  }
  await updateTreeDump();
}

async function treeDelete() {
  await tree.delete(treeElement.value).toPromise();
  const scanResult = await tree.simpleScan(1).toPromise();
  if (scanResult.length > 0) {
    treeElement.value = scanResult[0];
  }
  await updateTreeDump();
}

async function treeDelete10() {
  const entries = await tree.simpleScan(10).toPromise();
  // "randomize" the order
  entries.sort(() => Math.random() - 0.5);
  for (const entry of entries) {
    await tree.delete(entry).toPromise();
  }
  await updateTreeDump();
}
</script>

<template>
  <h2>B-Tree Test</h2>

  <div>
    <button @click="testBTree">Test B-tree</button>
    <button @click="bTreeBenchmark">Benchmark B-tree</button>
    <button @click="bTreeBenchmark2">Benchmark 2</button>
  </div>

  <div>
    <input type="text" v-model="treeElement" />
    <input type="number" v-model="treeMaxNodeSize" />
  </div>

  <div>
    <button @click="treeInit">init</button>
    <button @click="treeInsert">insert</button>
    <button @click="treeInsertRandom">insert random</button>
    <button @click="treeDelete">delete</button>
    <button @click="treeDelete10">delete 10</button>
  </div>

  <pre>{{ treeDump }}</pre>
</template>

<style scoped></style>
