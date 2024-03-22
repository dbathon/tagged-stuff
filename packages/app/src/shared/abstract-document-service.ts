import { DataStore } from "./data-store";
import { type Document } from "./document";

export abstract class AbstractDocumentService<D extends Document> {
  constructor(protected readonly dataStore: DataStore) {}

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
      throw new Error("invalid id for type " + this.typeName + ": " + id);
    }
    return id;
  }

  get(id: string): Promise<D | undefined> {
    this.validateId(id);
    return this.dataStore.get(id);
  }

  async save(document: D): Promise<void> {
    if (document.id === undefined) {
      // new document, generate the id before saving
      document.id = this.idPrefix + this.generateIdForNewDocument(document);
    }
    this.validateId(document.id);

    await this.dataStore.put(document);
  }

  delete(document: D): Promise<void> {
    return this.dataStore.delete(document);
  }

  query(): Promise<D[]> {
    // TODO hack...
    const maxIdExclusive = this.typeName + ".";
    return this.dataStore.scan(undefined, this.idPrefix, maxIdExclusive);
  }
}
