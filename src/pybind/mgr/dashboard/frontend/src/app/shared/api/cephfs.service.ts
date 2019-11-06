import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { CephfsDir } from '../models/cephfs-directory-models';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class CephfsService {
  baseURL = 'api/cephfs';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(`${this.baseURL}`);
  }

  lsDir(id, path?): Observable<CephfsDir[]> {
    let apiPath = `${this.baseURL}/${id}/ls_dir?depth=2`;
    if (path) {
      apiPath += `&path=${encodeURIComponent(path)}`;
    }
    return this.http.get<CephfsDir[]>(apiPath);
  }

  getCephfs(id) {
    return this.http.get(`${this.baseURL}/${id}`);
  }

  getTabs(id) {
    return this.http.get(`ui-api/cephfs/${id}/tabs`);
  }

  getClients(id) {
    return this.http.get(`${this.baseURL}/${id}/clients`);
  }

  evictClient(fsId, clientId) {
    return this.http.delete(`${this.baseURL}/${fsId}/client/${clientId}`);
  }

  getMdsCounters(id) {
    return this.http.get(`${this.baseURL}/${id}/mds_counters`);
  }
}
