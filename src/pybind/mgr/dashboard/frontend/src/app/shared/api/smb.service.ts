import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';

import { DomainSettings, SMBCluster, SMBShare } from '~/app/ceph/smb/smb.model';

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

  createCluster(requestModel: any) {
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

  create(requestModel: any) {
    return this.http.post(`${this.baseURL}/cluster`, requestModel);
  }

  createShare(requestModel: any) {
    return this.http.post(`${this.baseURL}/share`, requestModel);
  }
}
