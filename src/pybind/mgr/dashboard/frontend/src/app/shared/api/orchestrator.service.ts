import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class OrchestratorService {
  statusURL = 'api/orchestrator/status';
  inventoryURL = 'api/orchestrator/inventory';
  serviceURL = 'api/orchestrator/service';

  constructor(private http: HttpClient) {}

  status() {
    return this.http.get(this.statusURL);
  }

  inventoryList(hostname: string) {
    const options = hostname ? { params: new HttpParams().set('hostname', hostname) } : {};
    return this.http.get(this.inventoryURL, options);
  }

  serviceList(hostname: string) {
    const options = hostname ? { params: new HttpParams().set('hostname', hostname) } : {};
    return this.http.get(this.serviceURL, options);
  }
}
