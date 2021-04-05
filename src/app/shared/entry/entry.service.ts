import { AbstractDocumentService } from "../abstract-document-service";
import { Entry } from "./entry";

export class EntryService extends AbstractDocumentService<Entry> {

  protected readonly typeName = "entry";

  constructor(baseUrl: string) {
    super(baseUrl);
  }

}
