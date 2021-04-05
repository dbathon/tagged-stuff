import { Component, OnInit } from '@angular/core';
import { JdsClientService, DatabaseInformation } from '../shared/jds-client.service';
import { Entry } from "../shared/entry/entry";
import { EntryService } from "../shared/entry/entry.service";
import { FormBuilder, Validators } from "@angular/forms";
import { BTreeEntry, BTreeModificationResult, BTreeNode, BTreeScanParameters, RemoteBTree, Result } from "../shared/remote-b-tree";

class BTreeMap {

  readonly data: Map<string, BTreeNode> = new Map();
  readonly tree: RemoteBTree;
  readonly fetchNode: (id: string) => Result<BTreeNode>;
  rootId: string;
  fetchNodeWithPromise = false;

  constructor(treeOrder: number) {
    const fetchNodeRaw = (id: string): BTreeNode => {
      const node = this.data.get(id);
      if (node === undefined) {
        throw new Error("node not found: " + id);
      }
      return node;
    };
    const fetchNodePromise = async (id: string): Promise<BTreeNode> => fetchNodeRaw(id);
    const fetchNode = (id: string): Result<BTreeNode> => {
      if (this.fetchNodeWithPromise) {
        return Result.withPromise(fetchNodePromise(id));
      }
      else {
        return Result.withValue(fetchNodeRaw(id));
      }
    };
    this.fetchNode = fetchNode;
    let id = 0;
    const generateId = () => {
      ++id;
      return "" + id;
    };
    this.tree = new RemoteBTree(Math.max(treeOrder, 3), fetchNode, generateId);

    // dummy assignment for the type checker
    this.rootId = "";
    this.apply(this.tree.initializeNewTree());
  }

  apply(modificationResult: BTreeModificationResult): boolean {
    this.rootId = modificationResult.newRootId;
    modificationResult.obsoleteNodes.forEach(node => this.data.delete(node.id));
    modificationResult.newNodes.forEach(node => this.data.set(node.id, node));
    return modificationResult.obsoleteNodes.length > 0 || modificationResult.newNodes.length > 0;
  }

  get(key: string): Result<string | undefined> {
    return this.tree.getValue(key, this.rootId);
  }

  set(key: string, value: string): Result<boolean> {
    return this.tree.setValue(key, value, this.rootId).transform(result => this.apply(result));
  }

  delete(key: string): Result<boolean> {
    return this.tree.deleteKey(key, this.rootId).transform(result => this.apply(result));
  }

  scan(parameters?: BTreeScanParameters): Result<BTreeEntry[]> {
    return this.tree.scan(parameters || new BTreeScanParameters(), this.rootId);
  }

  getSize(): Result<number> {
    return this.tree.getSize(this.rootId);
  }

  clear() {
    this.data.clear();
    this.apply(this.tree.initializeNewTree());
  }

  private async dumpTreeInternal(nodeId: string, indent: string = "", sizeFromParent?: number): Promise<string[]> {
    const node = await this.fetchNode(nodeId).toPromise();
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
    this.jdsClient.getDatabaseInformation().then(info => this.databaseInformation = info);

    this.entryService.query().then(entries => this.entries = entries);
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
      this.entryService.save(this.activeEntry);
      this.activeEntry = undefined;
    }
  }

  deleteEntry(entry: Entry) {
    const index = this.entries.indexOf(entry);
    if (index >= 0) {
      this.entries.splice(index, 1);
      this.entryService.delete(entry);
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
      console.log(key, await tree.get(key).toPromise());
    }

    tree.clear();

    for (const key of [...chars].reverse()) {
      await tree.set(key, key.toUpperCase()).toPromise();
    }
    for (const key of chars) {
      await tree.set(key, key.toUpperCase()).toPromise();
    }
    console.log((await tree.dumpTree()).join("\n"));

    for (const key of chars) {
      console.log(key, await tree.get(key));
    }

    tree.clear();
    const testSize = 2000;
    for (let i = 0; i < testSize; ++i) {
      await tree.set("" + i, "" + i).toPromise();
    }
    console.log((await tree.dumpTree()).join("\n"));

    console.log(await tree.scan(new BTreeScanParameters(20, "2")));
    if ((await tree.scan().toPromise()).length != testSize || (await tree.getSize().toPromise()) != testSize) {
      throw new Error("scan failed");
    }

    for (let i = 0; i < testSize; ++i) {
      const key = "" + i;
      const result = await tree.scan(new BTreeScanParameters(1, key)).toPromise();
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
          await tree.set(str, str).toPromise();
        }
        if (await tree.getSize().toPromise() !== entryCount) {
          throw new Error();
        }
        for (let i = entryCount - 1; i >= 0; --i) {
          const str = "" + i;
          await tree.delete(str).toPromise();
        }
        if (await tree.getSize().toPromise() !== 0) {
          throw new Error();
        }

        const end = new Date().getTime();
        console.log(end - start, order, tree.rootId);
      }
    }
  }

  testSize = 5000;
  testOrder = 100;

  async bTreeBenchmark2() {
    for (const withPromise of [false, true]) {
      for (let j = 0; j < 4; ++j) {
        console.log("bTreeBenchmark2", withPromise, j);
        let start = new Date().getTime();
        const tree: BTreeMap = new BTreeMap(this.testOrder);
        tree.fetchNodeWithPromise = withPromise;
        const entryCount = this.testSize;
        for (let i = 0; i < entryCount; ++i) {
          const str = "" + i;
          const setResult = tree.set(str, str);
          if (!setResult.hasValue) {
            await setResult.promise;
          }
        }
        console.log("- insert done", new Date().getTime() - start);

        if (await tree.getSize().toPromise() !== entryCount) {
          throw new Error();
        }

        start = new Date().getTime();
        let result: string | undefined;
        try {
          for (let i = 0; i < entryCount; ++i) {
            const str = "" + i;
            const getResult = tree.get(str);
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

        if (await tree.getSize().toPromise() !== 0) {
          throw new Error();
        }
      }
    }
  }

  treeElement = "";
  treeOrder = "3";

  tree: BTreeMap = new BTreeMap(3);
  treeDump = "";

  private async updateTreeDump() {
    const entries = await this.tree.scan(new BTreeScanParameters()).toPromise();
    this.treeDump = "size: " + entries.length + ", " + (await this.tree.getSize().toPromise())
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
    await this.tree.set(this.treeElement, this.treeElement.toUpperCase()).toPromise();
    this.treeElement = "" + Math.floor(Math.random() * 10000);
    await this.updateTreeDump();
  }

  async treeInsertRandom() {
    for (let i = 0; i < 10; ++i) {
      const element: string = "" + Math.floor(Math.random() * 10000);
      await this.tree.set(element, element).toPromise();
    }
    await this.updateTreeDump();
  }

  async treeDelete() {
    await this.tree.delete(this.treeElement).toPromise();
    const firstEntry = (await this.tree.scan(new BTreeScanParameters(1)).toPromise())[0];
    if (firstEntry) {
      this.treeElement = firstEntry.key;
    }
    await this.updateTreeDump();
  }

  async treeDelete10() {
    const entries = await this.tree.scan(new BTreeScanParameters(10)).toPromise();
    // "randomize" the order
    entries.sort(() => Math.random() - 0.5);
    for (const entry of entries) {
      await this.tree.delete(entry.key).toPromise();
    }
    await this.updateTreeDump();
  }

}
