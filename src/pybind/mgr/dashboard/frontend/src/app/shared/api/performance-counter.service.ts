import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import 'rxjs/add/observable/of';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class PerformanceCounterService {

  private url = 'api/perf_counters';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(this.url);
  }

  get(service_type: string, service_id: string) {
    const serviceType = service_type.replace('-', '_');
    return this.http.get(`${this.url}/${serviceType}/${service_id}`)
      .flatMap((resp) => {
        return Observable.of(resp['counters']);
      });
  }
}
