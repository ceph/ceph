import { HttpClient } from '@angular/common/http';
import { inject, Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { catchError, map, shareReplay } from 'rxjs/operators';
import { MgrModuleService } from './mgr-module.service';
import { AuthStorageService } from '../services/auth-storage.service';
import { CompressionStats, HardwareHostResponse, HardwareSummary } from '../enum/hardware.enum';

@Injectable({
  providedIn: 'root'
})
export class HardwareService {
  baseURL = 'api/hardware';

  private readonly mgrModuleService = inject(MgrModuleService);
  private readonly authStorageService = inject(AuthStorageService);

  constructor(private http: HttpClient) {}

  /**
   * Shared observable — resolves once and replays.
   * True when cephadm hw_monitoring config flag is enabled.
   * Returns false when user lacks configOpt.read permission
   * or when cephadm is not available.
   */
  readonly isHardwareMonitoringEnabled$: Observable<boolean> =
    this.authStorageService.getPermissions().configOpt?.read
      ? this.mgrModuleService.getConfig('cephadm').pipe(
          map((resp: any) => !!resp?.hw_monitoring),
          catchError(() => of(false)),
          shareReplay({ bufferSize: 1, refCount: false })
        )
      : of(false);

  getSummary(category: string[] = []): Observable<HardwareSummary> {
    return this.http.get<HardwareSummary>(`${this.baseURL}/summary`, {
      params: { categories: category },
      headers: { Accept: 'application/vnd.ceph.api.v0.1+json' }
    });
  }

  getCompression(): Observable<CompressionStats> {
    return this.http.get<CompressionStats>(`${this.baseURL}/compression`, {
      headers: { Accept: 'application/vnd.ceph.api.v0.1+json' }
    });
  }

  /* Hardware report of one host */
  getHost(hostname: string): Observable<HardwareHostResponse> {
    return this.http.get<HardwareHostResponse>(
      `${this.baseURL}/hosts/${encodeURIComponent(hostname)}`
    );
  }
}
