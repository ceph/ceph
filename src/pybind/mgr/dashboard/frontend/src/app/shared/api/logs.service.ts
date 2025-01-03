import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class LogsService {
  constructor(private http: HttpClient) {}

  getLogs() {
    return this.http.get('api/logs/all');
  }

  validateDashboardUrl(uid: string) {
    return this.http.get(`api/grafana/validation/${uid}`);
  }
}
