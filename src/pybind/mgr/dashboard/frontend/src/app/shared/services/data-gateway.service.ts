import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';
import { JsonFormUISchema } from '../forms/crud-form/crud-form.model';
import { CrudFormAdapterService } from './crud-form-adapter.service';

@Injectable({
  providedIn: 'root'
})
export class DataGatewayService {
  cache: { [keys: string]: Observable<any> } = {};

  constructor(private http: HttpClient, private crudFormAdapater: CrudFormAdapterService) {}

  list(dataPath: string): Observable<any> {
    const cacheable = this.getCacheable(dataPath, 'get');
    if (this.cache[cacheable] === undefined) {
      const { url, version } = this.getUrlAndVersion(dataPath);

      this.cache[cacheable] = this.http.get<any>(url, {
        headers: { Accept: `application/vnd.ceph.api.v${version}+json` }
      });
    }

    return this.cache[cacheable];
  }

  create(dataPath: string, data: any): Observable<any> {
    const { url, version } = this.getUrlAndVersion(dataPath);

    return this.http.post<any>(url, data, {
      headers: { Accept: `application/vnd.ceph.api.v${version}+json` }
    });
  }

  delete(dataPath: string, key: string): Observable<any> {
    const { url, version } = this.getUrlAndVersion(dataPath);

    return this.http.delete<any>(`${url}/${key}`, {
      headers: { Accept: `application/vnd.ceph.api.v${version}+json` },
      observe: 'response'
    });
  }

  form(dataPath: string, formPath: string): Observable<JsonFormUISchema> {
    const cacheable = this.getCacheable(dataPath, 'get');
    if (this.cache[cacheable] === undefined) {
      const { url, version } = this.getUrlAndVersion(dataPath);

      this.cache[cacheable] = this.http.get<any>(url, {
        headers: { Accept: `application/vnd.ceph.api.v${version}+json` }
      });
    }
    return this.cache[cacheable].pipe(
      map((response) => {
        return this.crudFormAdapater.processJsonSchemaForm(response, formPath);
      })
    );
  }

  getCacheable(dataPath: string, method: string) {
    return dataPath + method;
  }

  getUrlAndVersion(dataPath: string) {
    const match = dataPath.match(/(?<url>[^@]+)(?:@(?<version>.+))?/);
    const url = match.groups.url.split('.').join('/');
    const version = match.groups.version || '1.0';

    return { url: url, version: version };
  }
}
