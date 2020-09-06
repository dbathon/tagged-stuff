import { Component, OnInit } from '@angular/core';
import { JdsClientService, DatabaseInformation } from '../shared/jds-client.service';
import { Entry } from "../shared/entry/entry";
import { EntryService } from "../shared/entry/entry.service";

@Component({
  selector: 'app-entries',
  templateUrl: './entries.component.html',
  styles: [
  ],
})
export class EntriesComponent implements OnInit {

  databaseInformation?: DatabaseInformation;

  entries: Entry[] = [];

  activeEntry?: Entry;

  constructor(private jdsClient: JdsClientService, private entryService: EntryService) { }

  ngOnInit(): void {
    this.jdsClient.getDatabaseInformation().subscribe(
      info => this.databaseInformation = info
    );

    this.entryService.query().subscribe(entries => this.entries = entries);
  }

  newEntry() {
    this.activeEntry = {};
  }

  editEntry(entry: Entry) {
    this.activeEntry = entry;
  }

  saveEntry() {
    if (this.activeEntry) {
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

}
