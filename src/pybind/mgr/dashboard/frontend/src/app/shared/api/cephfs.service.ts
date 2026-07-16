import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { Observable } from 'rxjs';

import { cdEncode, cdEncodeNot } from '../decorators/cd-encode';
import { CephfsDir, CephfsQuotas } from '../models/cephfs-directory-models';
import { shareReplay } from 'rxjs/operators';
import { Daemon, MirrorCheckpointListResponse, MirrorCheckpointMutationResponse, MirrorPeerList, MirrorStatusResponse } from '../models/cephfs.model';

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

  create(name: string, serviceSpec: object, dataPool = '', metadataPool = '') {
    return this.http.post(
      this.baseURL,
      {
        name: name,
        service_spec: serviceSpec,
        data_pool: dataPool,
        metadata_pool: metadataPool
      },
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

  setAuth(fsName: string, clientId: string | number, caps: string[], rootSquash: boolean) {
    return this.http.put(`${this.baseURL}/auth`, {
      fs_name: fsName,
      client_id: `client.${clientId}`,
      caps: caps,
      root_squash: rootSquash
    });
  }

  getUsedPools(): Observable<number[]> {
    return this.http.get<number[]>(`${this.baseUiURL}/used-pools`);
  }

  listDaemonStatus(): Observable<Daemon[]> {
    return this.http.get<Daemon[]>(`${this.baseURL}/mirror/daemon-status`);
  }

  enableMirror(@cdEncodeNot fsName: string): Observable<any> {
    return this.http.post(`${this.baseURL}/mirror/enable`, {
      fs_name: fsName
    });
  }

  createBootstrapToken(fsName: string, clientName: string, siteName: string): Observable<any> {
    return this.http.post(`${this.baseURL}/mirror/token`, {
      fs_name: fsName,
      client_name: clientName,
      site_name: siteName
    });
  }

  createBootstrapPeer(@cdEncodeNot fsName: string, @cdEncodeNot token: string): Observable<any> {
    return this.http.post(`${this.baseURL}/mirror`, {
      fs_name: fsName,
      token: token
    });
  }

  listMirrorPeers(fsName: string): Observable<MirrorPeerList> {
    return this.http.get<MirrorPeerList>(`${this.baseURL}/mirror/${fsName}`);
  }

  getMirrorStatus(
    fsName: string,
    path?: string,
    peerId?: string
  ): Observable<MirrorStatusResponse> {
    let params = new HttpParams();
    if (path) {
      params = params.set('path', path);
    }
    if (peerId) {
      params = params.set('peer_id', peerId);
    }
    return this.http.get<MirrorStatusResponse>(`${this.baseURL}/mirror/${fsName}/status`, {
      params
    });
  }

  addMirrorDirectory(@cdEncodeNot fsName: string, @cdEncodeNot path: string): Observable<any> {
    return this.http.post(`${this.baseURL}/mirror/directory`, {
      fs_name: fsName,
      path: path
    });
  }

  listMirrorDirectories(@cdEncodeNot fsName: string): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseURL}/mirror/directory/${fsName}`);
  }

  removeMirrorDirectory(@cdEncodeNot fsName: string, @cdEncodeNot path: string): Observable<any> {
    return this.http.delete(`${this.baseURL}/mirror/directory`, {
      params: { fs_name: fsName, path }
    });
  }

  listMirrorCheckpoints(
    @cdEncodeNot fsName: string,
    @cdEncodeNot path: string
  ): Observable<MirrorCheckpointListResponse> {
    return this.http.get<MirrorCheckpointListResponse>(
      `${this.baseURL}/mirror/${fsName}/checkpoint`,
      {
        params: { path }
      }
    );
  }

  addMirrorCheckpoint(
    @cdEncodeNot fsName: string,
    @cdEncodeNot path: string,
    @cdEncodeNot snapName: string
  ): Observable<MirrorCheckpointMutationResponse> {
    return this.http.post<MirrorCheckpointMutationResponse>(
      `${this.baseURL}/mirror/${fsName}/checkpoint`,
      {
        path,
        snap_name: snapName
      }
    );
  }

  createMirrorCheckpointNow(
    @cdEncodeNot fsName: string,
    @cdEncodeNot path: string
  ): Observable<MirrorCheckpointMutationResponse> {
    return this.http.post<MirrorCheckpointMutationResponse>(
      `${this.baseURL}/mirror/${fsName}/checkpoint/now`,
      {
        path
      }
    );
  }

  removeMirrorCheckpoint(
    @cdEncodeNot fsName: string,
    @cdEncodeNot path: string,
    @cdEncodeNot snapName: string
  ): Observable<MirrorCheckpointMutationResponse> {
    return this.http.delete<MirrorCheckpointMutationResponse>(
      `${this.baseURL}/mirror/${fsName}/checkpoint`,
      {
        params: { path, snap_name: snapName }
      }
    );
  }
}
