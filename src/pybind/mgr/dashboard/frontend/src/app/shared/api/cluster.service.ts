import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { CephClusterUser } from '~/app/shared/models/cluster.model';

@Injectable({
  providedIn: 'root'
})
export class ClusterService {
  baseURL = 'api/cluster';

  constructor(private http: HttpClient) {}

  getStatus(): Observable<string> {
    return this.http.get<string>(`${this.baseURL}`, {
      headers: { Accept: 'application/vnd.ceph.api.v0.1+json' }
    });
  }

  updateStatus(status: string) {
    return this.http.put(
      `${this.baseURL}`,
      { status: status },
      { headers: { Accept: 'application/vnd.ceph.api.v0.1+json' } }
    );
  }

  listUser(): Observable<CephClusterUser[]> {
    return this.http.get<CephClusterUser[]>(`${this.baseURL}/user`);
  }

  createUser(payload: CephClusterUser) {
    return this.http.post(`${this.baseURL}/user`, payload);
  }
}
