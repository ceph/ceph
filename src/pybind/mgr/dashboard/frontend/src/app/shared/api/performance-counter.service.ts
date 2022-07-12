import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { of as observableOf } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { cdEncode } from '../decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class PerformanceCounterService {
  private url = 'api/perf_counters';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(this.url);
  }

  get(service_type: string, service_id: string) {
    return this.http.get(`${this.url}/${service_type}/${service_id}`).pipe(
      mergeMap((resp: any) => {
        return observableOf(resp['counters']);
      })
    );
  }
}
