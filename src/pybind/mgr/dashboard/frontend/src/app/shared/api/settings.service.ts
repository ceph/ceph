import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class SettingsService {
  constructor(private http: HttpClient) {}

  getGrafanaApiUrl() {
    return this.http.get('api/settings/GRAFANA_API_URL');
  }
}
