import { Injectable } from '@angular/core';
import { SettingsService } from './settings.service';

export interface DatabaseInformation {
  name: string;
  version: string;
}

export interface JdsDocument {
  id?: string;
  version?: string;
}

interface QueryResult<D extends JdsDocument> {
  result: D[];
}

const ID_REGEXP = /^[a-zA-Z0-9][a-zA-Z0-9_\-]{0,199}$/;

@Injectable({
  providedIn: 'root'
})
export class JdsClientService {

  baseUrl?: string;

  constructor(settingsService: SettingsService) {
    settingsService.settings$.subscribe(settings => this.baseUrl = settings?.jdsUrl);
  }

  private getUrl(path: string, baseUrl = this.baseUrl) {
    if (!baseUrl) {
      throw new Error("no baseUrl");
    }
    const baseUrlWithSlash = baseUrl.endsWith('/') ? baseUrl : baseUrl + '/';
    // TODO maybe handle multiple slashes...
    const pathWithoutSlash = path.startsWith('/') ? path.substr(1) : path;
    return baseUrlWithSlash + pathWithoutSlash;
  }

  private async request<T>(method: "GET" | "POST" | "PUT" | "DELETE", url: string, body?: any): Promise<T> {
    const init: RequestInit = {
      method
    };
    if (body !== undefined) {
      init.body = JSON.stringify(body);
      init.headers = {
        "Content-Type": "application/json"
      };
    }
    const response = await fetch(url, init);
    if (!response.ok) {
      throw new Error(`request to ${url} failed with status ${response.status}`);
    }
    return response.json() as Promise<T>;
  }

  getDatabaseInformation(): Promise<DatabaseInformation> {
    return this.request<DatabaseInformation>("GET", this.getUrl(''));
  }

  private validateId(id?: string): string {
    if (id === undefined || !ID_REGEXP.test(id)) {
      throw new Error('invalid id: ' + id);
    }
    return id;
  }

  get<D extends JdsDocument>(id: string): Promise<D> {
    this.validateId(id);
    return this.request<D>("GET", this.getUrl(id));
  }

  put(document: JdsDocument): Promise<JdsDocument> {
    const id = this.validateId(document.id);
    return this.request<JdsDocument>("PUT", this.getUrl(id), document);
  }

  extractIdAndVersion(idOrDocument: string | undefined | JdsDocument): JdsDocument {
    if (typeof idOrDocument === "string" || idOrDocument === undefined) {
      return {
        id: this.validateId(idOrDocument)
      };
    }
    else {
      this.validateId(idOrDocument.id);
      // just return the given document
      return idOrDocument;
    }
  }

  delete(idOrDocument: string | undefined | JdsDocument): Promise<Object> {
    const document = this.extractIdAndVersion(idOrDocument);
    let url = this.getUrl(document.id!);
    if (document.version !== undefined) {
      url += "?" + new URLSearchParams({ version: document.version });
    }
    return this.request<Object>("DELETE", url);
  }

  async query<D extends JdsDocument>(filters?: any): Promise<D[]> {
    let url = this.getUrl("_query");
    if (filters !== undefined) {
      url += "?" + new URLSearchParams({ filters: JSON.stringify(filters) });
    }
    return (await this.request<QueryResult<D>>("GET", url)).result;
  }

}
