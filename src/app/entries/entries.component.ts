import { Component, OnInit } from '@angular/core';
import { JdsClientService, DatabaseInformation } from '../shared/jds-client.service';
import { Entry } from "../shared/entry/entry";
import { EntryService } from "../shared/entry/entry.service";
import { FormBuilder, Validators } from "@angular/forms";
import { BTreeNode, RemoteBTree } from "../shared/remote-b-tree";

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
    const tree = new RemoteBTree(10, id => {
      const node = nodesMap.get(id);
      if (node === undefined) {
        throw new Error("node not found: " + id);
      }
      return node;
    }, () => "bla");

    "abcdefghijklmnopq".split("").forEach(key => {
      console.log(key, tree.getValue(key, "root"));
    });
  }

}
