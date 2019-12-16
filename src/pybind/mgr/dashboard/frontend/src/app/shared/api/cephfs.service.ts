import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { Observable } from 'rxjs';

import { cdEncode } from '../decorators/cd-encode';
import { CephfsDir, CephfsQuotas } from '../models/cephfs-directory-models';
import { ApiModule } from './api.module';

@cdEncode
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

  mkSnapshot(id, path, name?) {
    let params = new HttpParams();
    params = params.append('path', path);
    if (!_.isUndefined(name)) {
      params = params.append('name', name);
    }
    return this.http.post(`${this.baseURL}/${id}/mk_snapshot`, null, { params });
  }

  rmSnapshot(id, path, name) {
    let params = new HttpParams();
    params = params.append('path', path);
    params = params.append('name', name);
    return this.http.post(`${this.baseURL}/${id}/rm_snapshot`, null, { params });
  }

  updateQuota(id, path, quotas: CephfsQuotas) {
    let params = new HttpParams();
    params = params.append('path', path);
    return this.http.post(`${this.baseURL}/${id}/set_quotas`, quotas, {
      observe: 'response',
      params
    });
  }
}
