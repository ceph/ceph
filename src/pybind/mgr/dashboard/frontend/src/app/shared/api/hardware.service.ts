import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class HardwareService {
  baseURL = 'api/hardware';

  constructor(private http: HttpClient) {}

  getSummary(category: string[] = []): any {
    return this.http.get<any>(`${this.baseURL}/summary`, {
      params: { categories: category },
      headers: { Accept: 'application/vnd.ceph.api.v0.1+json' }
    });
  }
}
