import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class CephfsService {
  baseURL = 'api/cephfs';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(`${this.baseURL}`);
  }

  getCephfs(id) {
    return this.http.get(`${this.baseURL}/${id}`);
  }

  getTabs(id) {
    return this.http.get(`ui-api/cephfs/${id}/tabs`);
  }

  getClients(id) {
    return this.http.get(`${this.baseURL}/${id}/clients`);
  }

  evictClient(fsId, clientId) {
    return this.http.delete(`${this.baseURL}/${fsId}/client/${clientId}`);
  }

  getMdsCounters(id) {
    return this.http.get(`${this.baseURL}/${id}/mds_counters`);
  }
}
