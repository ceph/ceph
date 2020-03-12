import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable, timer } from 'rxjs';
import { observeOn, shareReplay, switchMap } from 'rxjs/operators';

import { NgZoneSchedulerService } from './ngzone-scheduler.service';

export class FeatureTogglesMap {
  rbd = true;
  mirroring = true;
  iscsi = true;
  cephfs = true;
  rgw = true;
  nfs = true;
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

  constructor(private http: HttpClient, protected ngZone: NgZoneSchedulerService) {
    this.featureToggleMap$ = timer(0, this.REFRESH_INTERVAL, ngZone.leave).pipe(
      switchMap(() => this.http.get<FeatureTogglesMap>(this.API_URL)),
      shareReplay(1),
      observeOn(ngZone.enter)
    );
  }

  get(): FeatureTogglesMap$ {
    return this.featureToggleMap$;
  }
}
