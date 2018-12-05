import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class LogsService {
  constructor(private http: HttpClient) {}

  getLogs() {
    return this.http.get('api/logs/all');
  }

  validateDashboardUrl(uid) {
    return this.http.get(`api/grafana/validation/${uid}`);
  }
}
