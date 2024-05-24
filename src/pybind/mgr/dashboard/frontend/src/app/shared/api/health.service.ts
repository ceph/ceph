import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class HealthService {
  constructor(private http: HttpClient) {}

  getFullHealth() {
    return this.http.get('api/health/full');
  }

  getMinimalHealth() {
    return this.http.get('api/health/minimal');
  }

  getClusterCapacity() {
    return this.http.get('api/health/get_cluster_capacity');
  }

  getClusterFsid() {
    return this.http.get('api/health/get_cluster_fsid');
  }

  getOrchestratorName() {
    return this.http.get('api/health/get_orchestrator_name');
  }
}
