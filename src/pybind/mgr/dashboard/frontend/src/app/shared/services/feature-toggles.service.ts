import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable, timer } from 'rxjs';
import { flatMap, shareReplay } from 'rxjs/operators';

import { ServicesModule } from './services.module';

export type FeatureTogglesMap = Map<string, boolean>;
export type FeatureTogglesMap$ = Observable<FeatureTogglesMap>;

@Injectable({
  providedIn: ServicesModule
})
export class FeatureTogglesService {
  readonly API_URL: string = 'api/feature_toggles';
  readonly REFRESH_INTERVAL: number = 20000;
  private featureToggleMap$: FeatureTogglesMap$;

  constructor(private http: HttpClient) {
    this.featureToggleMap$ = timer(0, this.REFRESH_INTERVAL).pipe(
      flatMap(() => this.http.get<FeatureTogglesMap>(this.API_URL)),
      shareReplay(1)
    );
  }

  get(): FeatureTogglesMap$ {
    return this.featureToggleMap$;
  }
}
