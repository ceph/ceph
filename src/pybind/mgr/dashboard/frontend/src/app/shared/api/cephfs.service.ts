import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { Observable } from 'rxjs';

import { CephfsDir, CephfsQuotas } from '../models/cephfs-directory-models';
import { shareReplay } from 'rxjs/operators';
import {
  Daemon,
  MirrorCheckpointListResponse,
  MirrorCheckpointMutationResponse,
  MirrorPeerList,
  MirrorStatusResponse
} from '../models/cephfs.model';

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
    return this.http
      .get<CephfsDir[]>(`${this.baseUiURL}/${id}/ls_dir`, {
        params: path ? { depth, path } : { depth }
      })
      .pipe(shareReplay());
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
    return this.http.get(`${this.baseURL}/${encodeURIComponent(id)}/mds_counters`);
  }

  getFsRootDirectory(id: string) {
    return this.http.get(`${this.baseURL}/${encodeURIComponent(id)}/get_root_directory`);
  }

  mkSnapshot(id: number, path: string, name?: string) {
    return this.http.post(`${this.baseURL}/${id}/snapshot`, null, {
      params: name === undefined ? { path } : { path, name }
    });
  }

  rmSnapshot(id: number, path: string, name: string) {
    return this.http.delete(`${this.baseURL}/${id}/snapshot`, { params: { path, name } });
  }

  quota(id: number, path: string, quotas: CephfsQuotas) {
    return this.http.put(`${this.baseURL}/${id}/quota`, quotas, {
      observe: 'response',
      params: { path }
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
    return this.http.delete(`${this.baseURL}/remove/${encodeURIComponent(name)}`, {
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
    return this.http.get<Daemon[]>(`${this.baseURL}/mirror/daemon/status`);
  }

  enableMirror(fsName: string): Observable<any> {
    return this.http.post(`${this.baseURL}/mirror/enable`, {
      fs_name: fsName
    });
  }

  disableMirror(fsName: string): Observable<any> {
    return this.http.post(`${this.baseURL}/mirror/disable`, {
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

  createBootstrapPeer(fsName: string, token: string): Observable<any> {
    return this.http.post(`${this.baseURL}/mirror`, {
      fs_name: fsName,
      token: token
    });
  }

  private mirrorUrl(fsName: string): string {
    return `${this.baseURL}/mirror/${encodeURIComponent(fsName)}`;
  }

  listMirrorPeers(fsName: string): Observable<MirrorPeerList> {
    return this.http.get<MirrorPeerList>(this.mirrorUrl(fsName));
  }

  getMirrorStatus(
    fsName: string,
    path?: string,
    peerId?: string
  ): Observable<MirrorStatusResponse> {
    const params: Record<string, string> = {};
    if (path) {
      params.path = path;
    }
    if (peerId) {
      params.peer_id = peerId;
    }
    return this.http.get<MirrorStatusResponse>(`${this.mirrorUrl(fsName)}/status`, {
      params
    });
  }

  addMirrorDirectory(fsName: string, path: string): Observable<any> {
    return this.http.post(`${this.baseURL}/mirror/directory`, {
      fs_name: fsName,
      path: path
    });
  }

  listMirrorDirectories(fsName: string): Observable<string[]> {
    return this.http.get<string[]>(
      `${this.baseURL}/mirror/directory/${encodeURIComponent(fsName)}`
    );
  }

  removeMirrorDirectory(fsName: string, path: string): Observable<any> {
    return this.http.delete(`${this.baseURL}/mirror/directory`, {
      params: { fs_name: fsName, path }
    });
  }

  listMirrorCheckpoints(fsName: string, path: string): Observable<MirrorCheckpointListResponse> {
    return this.http.get<MirrorCheckpointListResponse>(`${this.mirrorUrl(fsName)}/checkpoint`, {
      params: { path }
    });
  }

  addMirrorCheckpoint(
    fsName: string,
    path: string,
    snapName: string
  ): Observable<MirrorCheckpointMutationResponse> {
    return this.http.post<MirrorCheckpointMutationResponse>(
      `${this.mirrorUrl(fsName)}/checkpoint`,
      {
        path,
        snap_name: snapName
      }
    );
  }

  removeMirrorCheckpoint(
    fsName: string,
    path: string,
    snapName: string
  ): Observable<MirrorCheckpointMutationResponse> {
    return this.http.delete<MirrorCheckpointMutationResponse>(
      `${this.mirrorUrl(fsName)}/checkpoint`,
      {
        params: { path, snap_name: snapName }
      }
    );
  }
}
