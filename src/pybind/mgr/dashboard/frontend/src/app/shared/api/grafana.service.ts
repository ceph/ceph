import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class GrafanaService {
  constructor(private http: HttpClient) {}

  getGrafanaApiUrl() {
    return this.http.get('api/grafana/url');
  }
}
