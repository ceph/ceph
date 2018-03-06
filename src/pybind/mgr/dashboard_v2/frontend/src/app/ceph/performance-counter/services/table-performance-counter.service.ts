import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable()
export class TablePerformanceCounterService {

  private url = 'api/perf_counters';

  constructor(private http: HttpClient) { }

  list() {
    return this.http.get(this.url)
      .toPromise()
      .then((resp: object): object => {
        return resp;
      });
  }

  get(service_type: string, service_id: string) {
    const serviceType = service_type.replace('-', '_');

    return this.http.get(`${this.url}/${serviceType}/${service_id}`)
      .toPromise()
      .then((resp: object): Array<object> => {
        return resp['counters'];
      });
  }
}
