import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { TimerService } from './timer.service';

export class FeatureTogglesMap {
  rbd = true;
  mirroring = true;
  iscsi = true;
  cephfs = true;
  rgw = true;
  nfs = true;
  dashboardV3 = true;
}
export type Features = keyof FeatureTogglesMap;
export type FeatureTogglesMap$ = Observable<FeatureTogglesMap>;

@Injectable({
  providedIn: 'root'
})
export class FeatureTogglesService {
  readonly API_URL: string = 'api/feature_toggles';
  readonly REFRESH_INTERVAL: number = 30000;
  private featureToggleMap$: FeatureTogglesMap$;

  constructor(private http: HttpClient, private timerService: TimerService) {
    this.featureToggleMap$ = this.timerService.get(
      () => this.http.get<FeatureTogglesMap>(this.API_URL),
      this.REFRESH_INTERVAL
    );
  }

  get(): FeatureTogglesMap$ {
    return this.featureToggleMap$;
  }
}
