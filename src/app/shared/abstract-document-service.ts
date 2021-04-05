import { JdsDocument, JdsClientService } from "./jds-client.service";

export abstract class AbstractDocumentService<D extends JdsDocument> {

  constructor(protected jdsClientService: JdsClientService) { }

  protected abstract readonly typeName: string;

  protected get idPrefix() {
    return this.typeName + "-";
  }

  /**
   * Generate the id (without the prefix part) for the given document.
   * <p>
   * The default implementation just returns the current time in millis as a string.
   *
   * @param document
   */
  protected generateIdForNewDocument(document: D): string {
    return `${new Date().getTime()}`;
  }

  private validateId(id?: string): string {
    if (id === undefined || !id.startsWith(this.idPrefix)) {
      throw new Error('invalid id for type ' + this.typeName + ': ' + id);
    }
    return id;
  }

  get(id: string): Promise<D> {
    this.validateId(id);
    return this.jdsClientService.get(id);
  }

  async save(document: D): Promise<D> {
    if (document.id === undefined) {
      // new document, generate the id before saving
      document.id = this.idPrefix + this.generateIdForNewDocument(document);
    }
    this.validateId(document.id);

    const responseDocument = await this.jdsClientService.put(document);

    // update the original document and return it
    if (document.id !== responseDocument.id) {
      throw new Error("unexpected id in response: " + document.id + ", " + responseDocument.id);
    }
    document.version = responseDocument.version;
    return document;
  }

  delete(idOrDocument: string | D): Promise<Object> {
    this.validateId(this.jdsClientService.extractIdAndVersion(idOrDocument).id);
    return this.jdsClientService.delete(idOrDocument);
  }

  query(): Promise<D[]> {
    const filters = {
      id: {
        ">=": this.idPrefix,
        // TODO hack...
        "<": this.typeName + "."
      }
    };
    return this.jdsClientService.query(filters);
  }

}
