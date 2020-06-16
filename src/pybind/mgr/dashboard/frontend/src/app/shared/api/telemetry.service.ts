import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class TelemetryService {
  private url = 'api/telemetry';

  constructor(private http: HttpClient) {}

  getReport() {
    return this.http.get(`${this.url}/report`);
  }

  enable(enable: boolean = true) {
    const body = { enable: enable };
    if (enable) {
      body['license_name'] = 'sharing-1-0';
    }
    return this.http.put(`${this.url}`, body);
  }
}
