import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable()
export class MonitorService {
  constructor(private http: HttpClient) {}

  getMonitor() {
    return this.http.get('api/monitor');
  }
}
