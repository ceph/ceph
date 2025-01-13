import { HttpClient, HttpResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';

import {
  ClusterRequestModel,
  DomainSettings,
  ShareRequestModel,
  SMBCluster,
  SMBJoinAuth,
  SMBShare,
  SMBUsersGroups
} from '~/app/ceph/smb/smb.model';

@Injectable({
  providedIn: 'root'
})
export class SmbService {
  baseURL = 'api/smb';
  private modalDataSubject = new Subject<DomainSettings>();
  modalData$ = this.modalDataSubject.asObservable();

  constructor(private http: HttpClient) {}

  passData(data: DomainSettings) {
    this.modalDataSubject.next(data);
  }

  listClusters(): Observable<SMBCluster[]> {
    return this.http.get<SMBCluster[]>(`${this.baseURL}/cluster`);
  }

  createCluster(requestModel: ClusterRequestModel) {
    return this.http.post(`${this.baseURL}/cluster`, requestModel);
  }

  removeCluster(clusterId: string) {
    return this.http.delete(`${this.baseURL}/cluster/${clusterId}`, {
      observe: 'response'
    });
  }

  listShares(clusterId: string): Observable<SMBShare[]> {
    return this.http.get<SMBShare[]>(`${this.baseURL}/share?cluster_id=${clusterId}`);
  }

  listJoinAuths(): Observable<SMBJoinAuth[]> {
    return this.http.get<SMBJoinAuth[]>(`${this.baseURL}/joinauth`);
  }

  listUsersGroups(): Observable<SMBUsersGroups[]> {
    return this.http.get<SMBUsersGroups[]>(`${this.baseURL}/usersgroups`);
  }

  createShare(requestModel: ShareRequestModel) {
    return this.http.post(`${this.baseURL}/share`, requestModel);
  }

  deleteShare(clusterId: string, shareId: string): Observable<HttpResponse<null>> {
    return this.http.delete<null>(`${this.baseURL}/share/${clusterId}/${shareId}`, {
      observe: 'response'
    });
  }
}
