import { Component, OnInit } from '@angular/core';
import { JdsClientService, DatabaseInformation } from '../shared/jds-client.service';
import { Entry } from "../shared/entry/entry";
import { EntryService } from "../shared/entry/entry.service";
import { FormBuilder, Validators } from "@angular/forms";
import { BTreeModificationResult, BTreeNode, RemoteBTree } from "../shared/remote-b-tree";

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
        children: ["c1", "c2"]
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
    const nodesMap: Map<string, BTreeNode> = new Map();
    nodes.forEach(node => nodesMap.set(node.id, node));

    const fetchNode = (id: string): BTreeNode => {
      const node = nodesMap.get(id);
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

    let rootId = "root";
    const dumpTree = (nodeId: string, indent: string = ""): string[] => {
      const node = fetchNode(nodeId);
      if (node.children) {
        let result: string[] = [];
        result.push(indent + nodeId + ":");
        const nextIndent = indent + "    ";
        for (let i = 0; i < node.keys.length; ++i) {
          result.push(...dumpTree(node.children[i], nextIndent));
          result.push(indent + "* " + node.keys[i]);
        }
        result.push(...dumpTree(node.children[node.keys.length], nextIndent));
        return result;
      }
      else {
        return [indent + nodeId + ": " + node.keys.join(", ")];
      }
    };

    const applyResult = (result: BTreeModificationResult) => {
      const changed = rootId !== result.newRootId;
      console.log("apply", changed, result.newNodes.length, result.obsoleteNodes.length);
      rootId = result.newRootId;
      result.newNodes.forEach(node => nodesMap.set(node.id, node));
      result.obsoleteNodes.forEach(node => nodesMap.delete(node.id));
    };

    const tree = new RemoteBTree(50, fetchNode, generateId);

    const chars = "abcdefghijklmnopqrstuvwxyz".split("");
    chars.forEach(key => {
      console.log(key, tree.getValue(key, rootId));
    });

    applyResult(tree.initializeNewTree());

    [...chars].reverse().forEach(key => {
      applyResult(tree.setValue(key, key.toUpperCase(), rootId));
    });
    chars.forEach(key => {
      applyResult(tree.setValue(key, key.toUpperCase(), rootId));
    });
    console.log(dumpTree(rootId).join("\n"));

    chars.forEach(key => {
      console.log(key, tree.getValue(key, rootId));
    });

    applyResult(tree.initializeNewTree());
    for (let i = 0; i < 2000; ++i) {
      applyResult(tree.setValue("" + i, "" + i, rootId));
    }
    console.log(dumpTree(rootId).join("\n"));
  }

}
