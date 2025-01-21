import { type Document } from "./document";

export interface DatabaseInformation {
  name: string;
  version: string;
}

interface QueryResult<D extends Document> {
  result: D[];
}

export interface MultiPutAndDelete {
  put?: Document[];
  delete?: Document[];
}

export class MultiPutAndDeleteResult {
  constructor(
    readonly newVersions: Record<string, string> | undefined,
    readonly errorDocumentId: string | undefined,
  ) {}
}

const ID_REGEXP = /^[a-zA-Z0-9][a-zA-Z0-9_\-]{0,199}$/;

export class JdsClient {
  constructor(readonly baseUrl: string) {}

  private getUrl(path: string) {
    const baseUrlWithSlash = this.baseUrl.endsWith("/") ? this.baseUrl : this.baseUrl + "/";
    // TODO maybe handle multiple slashes...
    const pathWithoutSlash = path.startsWith("/") ? path.substr(1) : path;
    return baseUrlWithSlash + pathWithoutSlash;
  }

  private requestRaw<T>(method: "GET" | "POST" | "PUT" | "DELETE", url: string, body?: any): Promise<Response> {
    const init: RequestInit = {
      method,
    };
    if (body !== undefined) {
      init.body = JSON.stringify(body);
      init.headers = {
        "Content-Type": "application/json",
      };
    }
    return fetch(url, init);
  }

  private createError(url: string, response: Response) {
    return new Error(`request to ${url} failed with status ${response.status}`);
  }

  private async request<T>(method: "GET" | "POST" | "PUT" | "DELETE", url: string, body?: any): Promise<T> {
    const response = await this.requestRaw(method, url, body);
    if (!response.ok) {
      throw this.createError(url, response);
    }
    return response.json() as Promise<T>;
  }

  getDatabaseInformation(): Promise<DatabaseInformation> {
    return this.request<DatabaseInformation>("GET", this.getUrl(""));
  }

  private validateId(id?: string): string {
    if (id === undefined || !ID_REGEXP.test(id)) {
      throw new Error("invalid id: " + id);
    }
    return id;
  }

  get<D extends Document>(id: string): Promise<D> {
    this.validateId(id);
    return this.request<D>("GET", this.getUrl(id));
  }

  put(document: Document): Promise<Document> {
    const id = this.validateId(document.id);
    return this.request<Document>("PUT", this.getUrl(id), document);
  }

  extractIdAndVersion(idOrDocument: string | undefined | Document): Document {
    if (typeof idOrDocument === "string" || idOrDocument === undefined) {
      return {
        id: this.validateId(idOrDocument),
      };
    } else {
      this.validateId(idOrDocument.id);
      // just return the given document
      return idOrDocument;
    }
  }

  delete(idOrDocument: string | undefined | Document): Promise<Object> {
    const document = this.extractIdAndVersion(idOrDocument);
    let url = this.getUrl(document.id!);
    if (document.version !== undefined) {
      url += "?" + new URLSearchParams({ version: document.version });
    }
    return this.request<Object>("DELETE", url);
  }

  async multiPutAndDelete(data: MultiPutAndDelete): Promise<MultiPutAndDeleteResult> {
    let url = this.getUrl("_multi");
    const response = await this.requestRaw<Object>("POST", url, data);
    if (response.ok) {
      const json = await response.json();
      const newVersions = json.newDocumentVersions as Record<string, string>;
      if (newVersions === undefined) {
        throw new Error("newDocumentVersions not present in response: " + JSON.stringify(json));
      }
      return new MultiPutAndDeleteResult(newVersions, undefined);
    } else if (response.status === 404 || response.status === 409) {
      const json = await response.json();
      const documentId = json.documentId as string;
      if (documentId === undefined) {
        throw new Error(
          "documentId not present in error response: " + JSON.stringify(json) + ", status: " + response.status,
        );
      }
      return new MultiPutAndDeleteResult(undefined, documentId);
    } else {
      throw this.createError(url, response);
    }
  }

  async query<D extends Document>(filters?: any): Promise<D[]> {
    let url = this.getUrl("_query");
    if (filters !== undefined) {
      url += "?" + new URLSearchParams({ filters: JSON.stringify(filters) });
    }
    return (await this.request<QueryResult<D>>("GET", url)).result;
  }
}
