import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { SettingsService } from './settings.service';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

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

  constructor(private httpClient: HttpClient, settingsService: SettingsService) {
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

  getDatabaseInformation() {
    return this.httpClient.get<DatabaseInformation>(this.getUrl(''));
  }

  private validateId(id?: string): string {
    if (id === undefined || !ID_REGEXP.test(id)) {
      throw new Error('invalid id: ' + id);
    }
    return id;
  }

  get<D extends JdsDocument>(id: string): Observable<D> {
    this.validateId(id);
    return this.httpClient.get<D>(this.getUrl(id));
  }

  put(document: JdsDocument): Observable<JdsDocument> {
    const id = this.validateId(document.id);
    return this.httpClient.put<JdsDocument>(this.getUrl(id), document);
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

  delete(idOrDocument: string | undefined | JdsDocument): Observable<Object> {
    const document = this.extractIdAndVersion(idOrDocument);
    let params = new HttpParams();
    if (document.version !== undefined) {
      params = params.set("version", document.version);
    }
    return this.httpClient.delete<Object>(this.getUrl(document.id!), { params });
  }

  query<D extends JdsDocument>(filters?: any): Observable<D[]> {
    let params = new HttpParams();
    if (filters !== undefined) {
      params = params.set("filters", JSON.stringify(filters));
      console.log("params", JSON.stringify(filters), params, filters);
    }
    return this.httpClient.get<QueryResult<D>>(this.getUrl("_query"), { params }).pipe(
      map(result => result.result)
    );
  }

}
