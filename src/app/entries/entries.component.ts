import { Component, OnInit } from '@angular/core';
import { JdsClientService, DatabaseInformation } from '../shared/jds-client.service';
import { Entry } from "../shared/entry/entry";
import { EntryService } from "../shared/entry/entry.service";
import { FormBuilder, Validators } from "@angular/forms";
import { BTreeEntry, BTreeModificationResult, BTreeNode, BTreeScanParameters, RemoteBTree, Result } from "../shared/remote-b-tree";
import { Observable, of, range } from "rxjs";
import { flatMap, last } from "rxjs/operators";

class BTreeMap {

  readonly data: Map<string, BTreeNode> = new Map();
  readonly tree: RemoteBTree;
  readonly fetchNode: (id: string) => Promise<BTreeNode>;
  rootId: string;
  fetchNode4WithPromise = false;

  constructor(treeOrder: number) {
    const fetchNode = async (id: string): Promise<BTreeNode> => {
      const node = this.data.get(id);
      if (node === undefined) {
        throw new Error("node not found: " + id);
      }
      return node;
    };
    this.fetchNode = fetchNode;
    const fetchNode2 = (id: string): Observable<BTreeNode> => {
      const node = this.data.get(id);
      if (node === undefined) {
        throw new Error("node not found: " + id);
      }
      return of(node);
    };
    const fetchNode3 = (id: string): BTreeNode => {
      const node = this.data.get(id);
      if (node === undefined) {
        throw new Error("node not found: " + id);
      }
      return node;
    };
    const fetchNode4 = (id: string): Result<BTreeNode> => {
      return this.fetchNode4WithPromise ? Result.withPromise(fetchNode(id)) : Result.withValue(fetchNode3(id));
    };
    let id = 0;
    const generateId = () => {
      ++id;
      return "" + id;
    };
    this.tree = new RemoteBTree(Math.max(treeOrder, 3), this.fetchNode, generateId, fetchNode2, fetchNode3, fetchNode4);

    // dummy assignment for the type checker
    this.rootId = "";
    this.apply(this.tree.initializeNewTree());
  }

  apply(modificationResult: BTreeModificationResult) {
    this.rootId = modificationResult.newRootId;
    modificationResult.obsoleteNodes.forEach(node => this.data.delete(node.id));
    modificationResult.newNodes.forEach(node => this.data.set(node.id, node));
  }

  async get(key: string): Promise<string | undefined> {
    return await this.tree.getValue(key, this.rootId);
  }

  get2(key: string): Observable<string | undefined> {
    return this.tree.getValue2(key, this.rootId);
  }

  get3(key: string): string | undefined {
    return this.tree.getValue3(key, this.rootId);
  }

  get4(key: string): Result<string | undefined> {
    return this.tree.getValue4(key, this.rootId);
  }

  async set(key: string, value: string) {
    this.apply(await this.tree.setValue(key, value, this.rootId));
  }

  async delete(key: string) {
    this.apply(await this.tree.deleteKey(key, this.rootId));
  }

  scan(parameters?: BTreeScanParameters): Promise<BTreeEntry[]> {
    return this.tree.scan(parameters || new BTreeScanParameters(), this.rootId);
  }

  getSize(): Promise<number> {
    return this.tree.getSize(this.rootId);
  }

  clear() {
    this.data.clear();
    this.apply(this.tree.initializeNewTree());
  }

  private async dumpTreeInternal(nodeId: string, indent: string = "", sizeFromParent?: number): Promise<string[]> {
    const node = await this.fetchNode(nodeId);
    const sizeFromParentString = sizeFromParent !== undefined ? " (" + sizeFromParent + ")" : "";
    if (node.children) {
      let result: string[] = [];
      result.push(indent + nodeId + sizeFromParentString + ":");
      const nextIndent = indent + "    ";
      for (let i = 0; i < node.keys.length; ++i) {
        result.push(...await this.dumpTreeInternal(node.children.ids[i], nextIndent, node.children.sizes[i]));
        result.push(indent + "* " + node.keys[i]);
      }
      result.push(...await this.dumpTreeInternal(node.children.ids[node.keys.length], nextIndent, node.children.sizes[node.keys.length]));
      return result;
    }
    else {
      return [indent + nodeId + sizeFromParentString + ": " + node.keys.join(", ")];
    }
  };

  dumpTree(): Promise<string[]> {
    return this.dumpTreeInternal(this.rootId);
  }

}

@Component({
  selector: 'app-entries',
  templateUrl: './entries.component.html',
  styles: [
  ],
})
export class EntriesComponent implements OnInit {

  editForm = this.formBuilder.group({
    title: [Validators.required]
  });

  databaseInformation?: DatabaseInformation;

  entries: Entry[] = [];

  activeEntry?: Entry;

  constructor(private jdsClient: JdsClientService, private entryService: EntryService, private formBuilder: FormBuilder) { }

  ngOnInit(): void {
    this.jdsClient.getDatabaseInformation().subscribe(
      info => this.databaseInformation = info
    );

    this.entryService.query().subscribe(entries => this.entries = entries);
  }

  newEntry() {
    this.editEntry({});
  }

  editEntry(entry: Entry) {
    this.activeEntry = entry;
    this.editForm.reset();
    this.editForm.setValue({ title: entry.title || "" });
  }

  saveEntry() {
    if (this.activeEntry && this.editForm.valid) {
      this.activeEntry.title = this.editForm.value.title;

      if (this.activeEntry.id === undefined) {
        this.entries.push(this.activeEntry);
      }
      this.entryService.save(this.activeEntry).subscribe();
      this.activeEntry = undefined;
    }
  }

  deleteEntry(entry: Entry) {
    const index = this.entries.indexOf(entry);
    if (index >= 0) {
      this.entries.splice(index, 1);
      this.entryService.delete(entry).subscribe();
    }
    if (this.activeEntry === entry) {
      this.activeEntry = undefined;
    }
  }

  async testBTree() {
    const nodes: BTreeNode[] = [
      {
        id: "root",
        keys: ["h"],
        values: ["H"],
        children: {
          ids: ["c1", "c2"],
          sizes: [3, 3]
        }
      },
      {
        id: "c1",
        keys: ["b", "d", "f"],
        values: ["B", "D", "F"],
      },
      {
        id: "c2",
        keys: ["m", "n", "o"],
        values: ["M", "N", "O"],
      }

    ];
    const tree = new BTreeMap(50);
    tree.data.clear();
    nodes.forEach(node => tree.data.set(node.id, node));
    tree.rootId = "root";

    const chars = "abcdefghijklmnopqrstuvwxyz".split("");
    for (const key of chars) {
      console.log(key, await tree.get(key));
    }

    tree.clear();

    for (const key of [...chars].reverse()) {
      await tree.set(key, key.toUpperCase());
    }
    for (const key of chars) {
      await tree.set(key, key.toUpperCase());
    }
    console.log((await tree.dumpTree()).join("\n"));

    for (const key of chars) {
      console.log(key, await tree.get(key));
    }

    tree.clear();
    const testSize = 2000;
    for (let i = 0; i < testSize; ++i) {
      await tree.set("" + i, "" + i);
    }
    console.log((await tree.dumpTree()).join("\n"));

    console.log(await tree.scan(new BTreeScanParameters(20, "2")));
    if ((await tree.scan()).length != testSize || (await tree.getSize()) != testSize) {
      throw new Error("scan failed");
    }

    for (let i = 0; i < testSize; ++i) {
      const key = "" + i;
      const result = await tree.scan(new BTreeScanParameters(1, key));
      if (!(result.length === 1 && result[0].key === key)) {
        throw new Error("scan failed");
      }
    }
  }

  async bTreeBenchmark() {
    for (const entryCount of [100, 500, 1000, 5000, 30000]) {
      console.log("start", entryCount);
      for (const order of [3, 5, 10, 30, 100, 500]) {
        const start = new Date().getTime();
        const tree: BTreeMap = new BTreeMap(order);
        for (let i = 0; i < entryCount; ++i) {
          const str = "" + i;
          await tree.set(str, str);
        }
        if (await tree.getSize() !== entryCount) {
          throw new Error();
        }
        for (let i = entryCount - 1; i >= 0; --i) {
          const str = "" + i;
          await tree.delete(str);
        }
        if (await tree.getSize() !== 0) {
          throw new Error();
        }

        const end = new Date().getTime();
        console.log(end - start, order, tree.rootId);
      }
    }
  }

  testSize = 5000;
  testOrder = 100;
  testTree: BTreeMap | undefined;

  async bTreeBenchmarkSetup() {
    const start = new Date().getTime();
    const tree: BTreeMap = new BTreeMap(this.testOrder);
    const entryCount = this.testSize;
    for (let i = 0; i < entryCount; ++i) {
      const str = "" + i;
      await tree.set(str, str);
    }
    if (await tree.getSize() !== entryCount) {
      throw new Error();
    }
    const insertDone = new Date().getTime();
    console.log("insert done", insertDone - start);
    this.testTree = tree;
  }

  async bTreeBenchmark1() {
    const start = new Date().getTime();
    let result: string | undefined;
    const entryCount = this.testSize;
    const tree = this.testTree;
    if (tree == undefined) {
      return;
    }
    for (let i = 0; i < entryCount; ++i) {
      const str = "" + i;
      result = await tree.get(str);
    }

    const end = new Date().getTime();
    console.log("getValuess", end - start, result);
  }

  bTreeBenchmark2() {
    const start = new Date().getTime();
    let result: string | undefined;
    const entryCount = this.testSize;
    const tree = this.testTree;
    if (tree == undefined) {
      return;
    }
    range(0, entryCount).pipe(
      flatMap(i => tree.get2("" + i)),
      last()
    ).subscribe(result => {
      const end = new Date().getTime();
      console.log("getValues", end - start, result);
    });
  }

  bTreeBenchmark3() {
    const start = new Date().getTime();
    let result: string | undefined;
    const entryCount = this.testSize;
    const tree = this.testTree;
    if (tree == undefined) {
      return;
    }
    for (let i = 0; i < entryCount; ++i) {
      const str = "" + i;
      result = tree.get3(str);
    }

    const end = new Date().getTime();
    console.log("getValues", end - start, result);
  }

  async bTreeBenchmark4() {
    let result: string | undefined;
    const entryCount = this.testSize;
    const tree = this.testTree;
    if (tree == undefined) {
      return;
    }
    for (const withPromise of [false, true]) {
      tree.fetchNode4WithPromise = withPromise;
      for (let j = 0; j < 4; ++j) {
        const start = new Date().getTime();
        try {
          for (let i = 0; i < entryCount; ++i) {
            const str = "" + i;
            const getResult = tree.get4(str);
            if (getResult.hasValue) {
              result = getResult.value;
            }
            else {
              result = await getResult.promise;
            }
          }
        }
        catch (e) {
          console.log(e);
        }

        const end = new Date().getTime();
        console.log("getValues", withPromise, j, end - start, result);
      }
    };
  }


  treeElement = "";
  treeOrder = "3";

  tree: BTreeMap = new BTreeMap(3);
  treeDump = "";

  private async updateTreeDump() {
    const entries = await this.tree.scan(new BTreeScanParameters());
    this.treeDump = "size: " + entries.length + ", " + (await this.tree.getSize())
      + "\norder: " + this.tree.tree.order
      + "\nentries: " + entries.map(entry => entry.key).join(", ")
      + "\nnodesMap size: " + this.tree.data.size
      + "\n\n" + (await this.tree.dumpTree()).join("\n");
  }

  async treeInit() {
    this.tree = new BTreeMap(Math.max(parseInt(this.treeOrder), 3));
    await this.updateTreeDump();
  }

  async treeInsert() {
    await this.tree.set(this.treeElement, this.treeElement.toUpperCase());
    this.treeElement = "" + Math.floor(Math.random() * 10000);
    await this.updateTreeDump();
  }

  async treeInsertRandom() {
    for (let i = 0; i < 10; ++i) {
      const element: string = "" + Math.floor(Math.random() * 10000);
      await this.tree.set(element, element);
    }
    await this.updateTreeDump();
  }

  async treeDelete() {
    this.tree.delete(this.treeElement);
    const firstEntry = (await this.tree.scan(new BTreeScanParameters(1)))[0];
    if (firstEntry) {
      this.treeElement = firstEntry.key;
    }
    await this.updateTreeDump();
  }

  async treeDelete10() {
    const entries = await this.tree.scan(new BTreeScanParameters(10));
    // "randomize" the order
    entries.sort(() => Math.random() - 0.5);
    for (const entry of entries) {
      await this.tree.delete(entry.key);
    }
    await this.updateTreeDump();
  }

}
