import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { HardwareSummary } from '../enum/hardware.enum';

@Injectable({
  providedIn: 'root'
})
export class HardwareService {
  baseURL = 'api/hardware';

  constructor(private http: HttpClient) {}

  getSummary(category: string[] = []): Observable<HardwareSummary> {
    return this.http.get<HardwareSummary>(`${this.baseURL}/summary`, {
      params: { categories: category },
      headers: { Accept: 'application/vnd.ceph.api.v0.1+json' }
    });
  }
}
