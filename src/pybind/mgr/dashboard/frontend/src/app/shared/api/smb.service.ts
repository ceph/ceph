import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, Subject } from 'rxjs';

import { SMBCluster } from '~/app/ceph/smb/smb.model';

@Injectable({
  providedIn: 'root'
})
export class SmbService {
  baseURL = 'api/smb';
  private modalDataSubject = new Subject<any>();
  modalData$ = this.modalDataSubject.asObservable();

  constructor(private http: HttpClient) {}

  passData(data: any) {
    this.modalDataSubject.next(data);
  }

  listClusters(): Observable<SMBCluster[]> {
    return this.http.get<SMBCluster[]>(`${this.baseURL}/cluster`);
  }

  create(requestModel: any) {
    return this.http.post(`${this.baseURL}/cluster`, requestModel);
  }
}
