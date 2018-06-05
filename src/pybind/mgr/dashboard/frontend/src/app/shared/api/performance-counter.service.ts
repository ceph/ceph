import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { of as observableOf } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class PerformanceCounterService {
  private url = 'api/perf_counters';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(this.url);
  }

  get(service_type: string, service_id: string) {
    const serviceType = service_type.replace('-', '_');
    return this.http.get(`${this.url}/${serviceType}/${service_id}`).pipe(
      mergeMap((resp) => {
        return observableOf(resp['counters']);
      })
    );
  }
}
