import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class MonitorService {
  constructor(private http: HttpClient) {}

  getMonitor() {
    return this.http.get('api/monitor');
  }
}
