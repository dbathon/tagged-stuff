import { Component, OnInit } from '@angular/core';
import { JdsClientService, DatabaseInformation } from '../shared/jds-client.service';
import { Entry } from "../shared/entry/entry";
import { EntryService } from "../shared/entry/entry.service";
import { FormBuilder, Validators } from "@angular/forms";
import { BTreeEntry, BTreeModificationResult, BTreeNode, BTreeScanParameters, RemoteBTree } from "../shared/remote-b-tree";

class BTreeMap {

  readonly data: Map<string, BTreeNode> = new Map();
  readonly tree: RemoteBTree;
  readonly fetchNode: (id: string) => BTreeNode;
  rootId: string;

  constructor(treeOrder: number) {
    this.fetchNode = (id: string): BTreeNode => {
      const node = this.data.get(id);
      if (node === undefined) {
        throw new Error("node not found: " + id);
      }
      return node;
    };
    let id = 0;
    const generateId = () => {
      ++id;
      return "" + id;
    };
    this.tree = new RemoteBTree(Math.max(treeOrder, 3), this.fetchNode, generateId);

    // dummy assignment for the type checker
    this.rootId = "";
    this.apply(this.tree.initializeNewTree());
  }

  apply(modificationResult: BTreeModificationResult) {
    this.rootId = modificationResult.newRootId;
    modificationResult.obsoleteNodes.forEach(node => this.data.delete(node.id));
    modificationResult.newNodes.forEach(node => this.data.set(node.id, node));
  }

  get(key: string): string | undefined {
    return this.tree.getValue(key, this.rootId);
  }

  set(key: string, value: string) {
    this.apply(this.tree.setValue(key, value, this.rootId));
  }

  delete(key: string) {
    this.apply(this.tree.deleteKey(key, this.rootId));
  }

  scan(parameters?: BTreeScanParameters): BTreeEntry[] {
    return this.tree.scan(parameters || new BTreeScanParameters(), this.rootId);
  }

  getSize(): number {
    return this.tree.getSize(this.rootId);
  }

  clear() {
    this.data.clear();
    this.apply(this.tree.initializeNewTree());
  }

  private dumpTreeInternal(nodeId: string, indent: string = "", sizeFromParent?: number): string[] {
    const node = this.fetchNode(nodeId);
    const sizeFromParentString = sizeFromParent !== undefined ? " (" + sizeFromParent + ")" : "";
    if (node.children) {
      let result: string[] = [];
      result.push(indent + nodeId + sizeFromParentString + ":");
      const nextIndent = indent + "    ";
      for (let i = 0; i < node.keys.length; ++i) {
        result.push(...this.dumpTreeInternal(node.children.ids[i], nextIndent, node.children.sizes[i]));
        result.push(indent + "* " + node.keys[i]);
      }
      result.push(...this.dumpTreeInternal(node.children.ids[node.keys.length], nextIndent, node.children.sizes[node.keys.length]));
      return result;
    }
    else {
      return [indent + nodeId + sizeFromParentString + ": " + node.keys.join(", ")];
    }
  };

  dumpTree(): string[] {
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

  testBTree() {
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
    chars.forEach(key => {
      console.log(key, tree.get(key));
    });

    tree.clear();

    [...chars].reverse().forEach(key => {
      tree.set(key, key.toUpperCase());
    });
    chars.forEach(key => {
      tree.set(key, key.toUpperCase());
    });
    console.log(tree.dumpTree().join("\n"));

    chars.forEach(key => {
      console.log(key, tree.get(key));
    });

    tree.clear();
    const testSize = 2000;
    for (let i = 0; i < testSize; ++i) {
      tree.set("" + i, "" + i);
    }
    console.log(tree.dumpTree().join("\n"));

    console.log(tree.scan(new BTreeScanParameters(20, "2")));
    if (tree.scan().length != testSize || tree.getSize() != testSize) {
      throw new Error("scan failed");
    }

    for (let i = 0; i < testSize; ++i) {
      const key = "" + i;
      const result = tree.scan(new BTreeScanParameters(1, key));
      if (!(result.length === 1 && result[0].key === key)) {
        throw new Error("scan failed");
      }
    }
  }

  treeElement = "";
  treeOrder = "3";

  tree: BTreeMap = new BTreeMap(3);

  get treeDump() {
    const entries = this.tree.scan(new BTreeScanParameters());
    return "size: " + entries.length + ", " + this.tree.getSize()
      + "\norder: " + this.tree.tree.order
      + "\nentries: " + entries.map(entry => entry.key).join(", ")
      + "\nnodesMap size: " + this.tree.data.size
      + "\n\n" + this.tree.dumpTree().join("\n");
  }

  treeInit() {
    this.tree = new BTreeMap(Math.max(parseInt(this.treeOrder), 3));
  }

  treeInsert() {
    this.tree.set(this.treeElement, this.treeElement.toUpperCase());
    this.treeElement = "" + Math.floor(Math.random() * 10000);
  }

  treeInsertRandom() {
    for (let i = 0; i < 10; ++i) {
      const element: string = "" + Math.floor(Math.random() * 10000);
      this.tree.set(element, element);
    }
  }

  treeDelete() {
    this.tree.delete(this.treeElement);
    const firstEntry = this.tree.scan(new BTreeScanParameters(1))[0];
    if (firstEntry) {
      this.treeElement = firstEntry.key;
    }
  }

  treeDelete10() {
    const entries = this.tree.scan(new BTreeScanParameters(10));
    // "randomize" the order
    entries.sort(() => Math.random() - 0.5);
    entries.forEach(entry =>
      this.tree.delete(entry.key)
    );
  }

}
