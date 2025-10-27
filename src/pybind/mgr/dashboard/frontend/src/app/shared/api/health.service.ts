import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HealthSnapshotMap } from '../models/health.interface';

const BASE_URL = 'api/health';

@Injectable({
  providedIn: 'root'
})
export class HealthService {
  constructor(private http: HttpClient) {}

  getFullHealth() {
    return this.http.get(`${BASE_URL}/full`);
  }

  getMinimalHealth() {
    return this.http.get(`${BASE_URL}/minimal`);
  }

  getHealthSnapshot(): Observable<HealthSnapshotMap> {
    return this.http.get<HealthSnapshotMap>(`${BASE_URL}/snapshot`);
  }

  getClusterFsid() {
    return this.http.get(`${BASE_URL}/get_cluster_fsid`);
  }

  getOrchestratorName() {
    return this.http.get(`${BASE_URL}/get_orchestrator_name`);
  }

  getTelemetryStatus() {
    return this.http.get(`${BASE_URL}/get_telemetry_status`);
  }
}
