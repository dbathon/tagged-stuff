import { Injectable } from '@angular/core';
import { AbstractDocumentService } from "../abstract-document-service";
import { Entry } from "./entry";
import { JdsClientService } from "../jds-client.service";

@Injectable({
  providedIn: 'root'
})
export class EntryService extends AbstractDocumentService<Entry> {

  protected readonly typeName = "entry";

  constructor(jdsClientService: JdsClientService) {
    super(jdsClientService);
  }

}
