import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class HostService {
  baseURL = 'api/host';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(this.baseURL);
  }

  add(hostname) {
    return this.http.post(this.baseURL, { hostname: hostname }, { observe: 'response' });
  }

  remove(hostname) {
    return this.http.delete(`${this.baseURL}/${hostname}`, { observe: 'response' });
  }
}
