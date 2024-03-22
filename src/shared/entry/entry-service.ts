import { AbstractDocumentService } from "../abstract-document-service";
import { DataStore } from "../data-store";
import { type Entry } from "./entry";

export class EntryService extends AbstractDocumentService<Entry> {
  protected readonly typeName = "entry";

  constructor(dataStore: DataStore) {
    super(dataStore);
  }
}
