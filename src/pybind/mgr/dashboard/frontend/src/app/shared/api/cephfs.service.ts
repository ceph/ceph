import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { Observable } from 'rxjs';

import { cdEncode } from '../decorators/cd-encode';
import { CephfsDir, CephfsQuotas } from '../models/cephfs-directory-models';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class CephfsService {
  baseURL = 'api/cephfs';
  baseUiURL = 'ui-api/cephfs';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(`${this.baseURL}`);
  }

  lsDir(id: number, path?: string): Observable<CephfsDir[]> {
    let apiPath = `${this.baseUiURL}/${id}/ls_dir?depth=2`;
    if (path) {
      apiPath += `&path=${encodeURIComponent(path)}`;
    }
    return this.http.get<CephfsDir[]>(apiPath);
  }

  getCephfs(id: number) {
    return this.http.get(`${this.baseURL}/${id}`);
  }

  getTabs(id: number) {
    return this.http.get(`ui-api/cephfs/${id}/tabs`);
  }

  getClients(id: number) {
    return this.http.get(`${this.baseURL}/${id}/clients`);
  }

  evictClient(fsId: number, clientId: number) {
    return this.http.delete(`${this.baseURL}/${fsId}/client/${clientId}`);
  }

  getMdsCounters(id: string) {
    return this.http.get(`${this.baseURL}/${id}/mds_counters`);
  }

  mkSnapshot(id: number, path: string, name?: string) {
    let params = new HttpParams();
    params = params.append('path', path);
    if (!_.isUndefined(name)) {
      params = params.append('name', name);
    }
    return this.http.post(`${this.baseURL}/${id}/mk_snapshot`, null, { params });
  }

  rmSnapshot(id: number, path: string, name: string) {
    let params = new HttpParams();
    params = params.append('path', path);
    params = params.append('name', name);
    return this.http.post(`${this.baseURL}/${id}/rm_snapshot`, null, { params });
  }

  updateQuota(id: number, path: string, quotas: CephfsQuotas) {
    let params = new HttpParams();
    params = params.append('path', path);
    return this.http.post(`${this.baseURL}/${id}/set_quotas`, quotas, {
      observe: 'response',
      params
    });
  }
}
