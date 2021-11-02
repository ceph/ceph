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
    if (this.cache[dataPath] === undefined) {
      const match = dataPath.match(/(?<url>[^@]+)(?:@(?<version>.+))?/);
      const url = match.groups.url.split('.').join('/');
      const version = match.groups.version || '1.0';

      this.cache[dataPath] = this.http.get<any>(url, {
        headers: { Accept: `application/vnd.ceph.api.v${version}+json` }
      });
    }

    return this.cache[dataPath];
  }
}
