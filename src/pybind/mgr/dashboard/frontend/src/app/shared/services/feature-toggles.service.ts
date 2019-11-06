import { HttpClient } from '@angular/common/http';
import { Injectable, NgZone } from '@angular/core';

import { Observable } from 'rxjs';
import { shareReplay } from 'rxjs/operators';

export type FeatureTogglesMap = Map<string, boolean>;
export type FeatureTogglesMap$ = Observable<FeatureTogglesMap>;

@Injectable({
  providedIn: 'root'
})
export class FeatureTogglesService {
  readonly API_URL: string = 'api/feature_toggles';
  readonly REFRESH_INTERVAL: number = 20000;
  private featureToggleMap$: FeatureTogglesMap$;

  constructor(private http: HttpClient, private zone: NgZone) {
    this.featureToggleMap$ = this.http.get<FeatureTogglesMap>(this.API_URL).pipe(shareReplay(1));
    this.zone.runOutsideAngular(() => {
      window.setInterval(() => {
        this.zone.run(() => {
          this.featureToggleMap$ = this.http
            .get<FeatureTogglesMap>(this.API_URL)
            .pipe(shareReplay(1));
        });
      }, this.REFRESH_INTERVAL);
    });
  }

  get(): FeatureTogglesMap$ {
    return this.featureToggleMap$;
  }
}
