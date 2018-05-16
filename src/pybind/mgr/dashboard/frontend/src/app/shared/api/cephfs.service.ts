import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable()
export class CephfsService {
  baseURL = 'api/cephfs';

  constructor(private http: HttpClient) {}

  getCephfs(id) {
    return this.http.get(`${this.baseURL}/data/${id}`);
  }

  getClients(id) {
    return this.http.get(`${this.baseURL}/clients/${id}`);
  }

  getMdsCounters(id) {
    return this.http.get(`${this.baseURL}/mds_counters/${id}`);
  }
}
