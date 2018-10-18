import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class DashboardService {
  constructor(private http: HttpClient) {}

  getHealth() {
    return this.http.get('api/dashboard/health');
  }
}
