import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable()
export class TablePerformanceCounterService {

  private url = '/api/perf_counters';

  constructor(private http: HttpClient) { }

  list() {
    return this.http.get(this.url)
      .toPromise()
      .then((resp: object): object => {
        return resp;
      });
  }

  get(service_type: string, service_id: string) {
    return this.http.get(`${this.url}/${service_type}/${service_id}`)
      .toPromise()
      .then((resp: object): Array<object> => {
        return resp['counters'];
      });
  }
}
