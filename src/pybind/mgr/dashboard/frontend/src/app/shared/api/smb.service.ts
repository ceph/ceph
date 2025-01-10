import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';

import { SMBJoinAuth, SMBCluster, SMBUsersgroups } from '~/app/ceph/smb/smb.model';

@Injectable({
  providedIn: 'root'
})
export class SmbService {
  baseURL = 'api/smb';

  constructor(private http: HttpClient) {}

  listClusters(): Observable<SMBCluster[]> {
    return this.http.get<SMBCluster[]>(`${this.baseURL}/cluster`);
  }

  listJoinAuths(): Observable<SMBJoinAuth[]> {
    return this.http.get<SMBJoinAuth[]>(`${this.baseURL}/joinauth`);
  }

  listUsersGroups(): Observable<SMBUsersgroups[]> {
    return this.http.get<SMBUsersgroups[]>(`${this.baseURL}/usersgroups`);
  }

  createJoinAuth(authId: string, username: string, password: string) {
    return this.http.post(`${this.baseURL}/joinauth`, {
      auth_id: authId,
      username: username,
      password: password
    })
  }

  createUsersgroups(usersgroups: SMBUsersgroups) {
    return this.http.post(`${this.baseURL}/usersgroups`, {
      usersgroups: usersgroups
    })
  }
}
