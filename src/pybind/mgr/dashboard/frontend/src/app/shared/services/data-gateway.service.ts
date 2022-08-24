import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class DataGatewayService {
  cache: { [keys: string]: Observable<any> } = {};

  constructor(private http: HttpClient) {}

  list(dataPath: string): Observable<any> {
    const cacheable = this.getCacheable(dataPath, 'get');
    if (this.cache[cacheable] === undefined) {
      const match = dataPath.match(/(?<url>[^@]+)(?:@(?<version>.+))?/);
      const url = match.groups.url.split('.').join('/');
      const version = match.groups.version || '1.0';

      this.cache[cacheable] = this.http.get<any>(url, {
        headers: { Accept: `application/vnd.ceph.api.v${version}+json` }
      });
    }

    return this.cache[cacheable];
  }

  create(dataPath: string, data: any): Observable<any> {
    const match = dataPath.match(/(?<url>[^@]+)(?:@(?<version>.+))?/);
    const url = match.groups.url.split('.').join('/');
    const version = match.groups.version || '1.0';

    return this.http.post<any>(url, data, {
      headers: { Accept: `application/vnd.ceph.api.v${version}+json` }
    });
  }

  getCacheable(dataPath: string, method: string) {
    return dataPath + method;
  }
}
