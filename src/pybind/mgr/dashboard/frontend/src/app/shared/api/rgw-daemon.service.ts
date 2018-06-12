import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class RgwDaemonService {
  private url = 'api/rgw/daemon';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(this.url);
  }

  get(id: string) {
    return this.http.get(`${this.url}/${id}`);
  }
}
