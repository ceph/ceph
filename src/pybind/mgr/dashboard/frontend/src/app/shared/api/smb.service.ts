import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

import { JoinAuth, SMBCluster, UsersGroups } from '~/app/ceph/smb/smb.model';

@Injectable({
  providedIn: 'root'
})
export class SmbService {
  baseURL = 'api/smb';

  constructor(private http: HttpClient) {}

  listClusters(): Observable<SMBCluster[]> {
    return this.http.get<SMBCluster[]>(`${this.baseURL}/cluster`);
  }

  listJoinAuths(): Observable<JoinAuth[]> {
    return this.http.get<JoinAuth[]>(`${this.baseURL}/joinauth`);
  }

  listUsersGroups(): Observable<UsersGroups[]> {
    return this.http.get<UsersGroups[]>(`${this.baseURL}/usersgroups`);
  }
}
