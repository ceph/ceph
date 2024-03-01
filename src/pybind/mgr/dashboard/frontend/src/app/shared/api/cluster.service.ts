import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

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
}
