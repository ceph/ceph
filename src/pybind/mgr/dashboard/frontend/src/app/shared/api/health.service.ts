import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { IscsiMap, MdsMap, MgrMap, MonMap, OsdMap, PgStatus } from '../models/health.interface';

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

  getTelemetryStatus() {
    return this.http.get('api/health/get_telemetry_status');
  }

  getHostsCount(): Observable<number> {
    return this.http.get<number>('api/health/status/hosts');
  }

  getMonMap(): Observable<MonMap> {
    return this.http.get<MonMap>('api/health/status/mon');
  }

  getMgrMap(): Observable<MgrMap> {
    return this.http.get<MgrMap>('api/health/status/mgr');
  }

  getOsdMap(): Observable<OsdMap> {
    return this.http.get<OsdMap>('api/health/status/osd');
  }

  getPoolStatus(): Observable<any> {
    return this.http.get<any>('api/health/status/pool');
  }

  getPgInfo(): Observable<PgStatus> {
    return this.http.get<PgStatus>('api/health/status/pg');
  }

  getRgwStatus(): Observable<number> {
    return this.http.get<number>('api/health/status/rgw');
  }

  getMdsMap(): Observable<MdsMap> {
    return this.http.get<MdsMap>('api/health/status/mds');
  }

  getIscsiMap(): Observable<IscsiMap> {
    return this.http.get<IscsiMap>('api/health/status/iscsi');
  }

  getBasicHealth(): Observable<any> {
    return this.http.get<any>('api/health/status/basic');
  }
}
