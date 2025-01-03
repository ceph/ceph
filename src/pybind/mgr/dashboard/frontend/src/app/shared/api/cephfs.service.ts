import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { Observable } from 'rxjs';

import { cdEncode } from '../decorators/cd-encode';
import { CephfsDir, CephfsQuotas } from '../models/cephfs-directory-models';
import { shareReplay } from 'rxjs/operators';

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

  lsDir(id: number, path?: string, depth: number = 2): Observable<CephfsDir[]> {
    let apiPath = `${this.baseUiURL}/${id}/ls_dir?depth=${depth}`;
    if (path) {
      apiPath += `&path=${encodeURIComponent(path)}`;
    }
    return this.http.get<CephfsDir[]>(apiPath).pipe(shareReplay());
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

  getFsRootDirectory(id: string) {
    return this.http.get(`${this.baseURL}/${id}/get_root_directory`);
  }

  mkSnapshot(id: number, path: string, name?: string) {
    let params = new HttpParams();
    params = params.append('path', path);
    if (!_.isUndefined(name)) {
      params = params.append('name', name);
    }
    return this.http.post(`${this.baseURL}/${id}/snapshot`, null, { params });
  }

  rmSnapshot(id: number, path: string, name: string) {
    let params = new HttpParams();
    params = params.append('path', path);
    params = params.append('name', name);
    return this.http.delete(`${this.baseURL}/${id}/snapshot`, { params });
  }

  quota(id: number, path: string, quotas: CephfsQuotas) {
    let params = new HttpParams();
    params = params.append('path', path);
    return this.http.put(`${this.baseURL}/${id}/quota`, quotas, {
      observe: 'response',
      params
    });
  }

  create(name: string, serviceSpec: object) {
    return this.http.post(
      this.baseURL,
      { name: name, service_spec: serviceSpec },
      {
        observe: 'response'
      }
    );
  }

  isCephFsPool(pool: any) {
    return _.indexOf(pool.application_metadata, 'cephfs') !== -1 && !pool.pool_name.includes('/');
  }

  remove(name: string) {
    return this.http.delete(`${this.baseURL}/remove/${name}`, {
      observe: 'response'
    });
  }

  rename(vol_name: string, new_vol_name: string) {
    let requestBody = {
      name: vol_name,
      new_name: new_vol_name
    };
    return this.http.put(`${this.baseURL}/rename`, requestBody, {
      observe: 'response'
    });
  }

  setAuth(fsName: string, clientId: number, caps: string[], rootSquash: boolean) {
    return this.http.put(`${this.baseURL}/auth`, {
      fs_name: fsName,
      client_id: `client.${clientId}`,
      caps: caps,
      root_squash: rootSquash
    });
  }
}
