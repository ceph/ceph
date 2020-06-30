import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { Daemon } from '../models/daemon.interface';
import { CephServiceSpec } from '../models/service.interface';

@Injectable({
  providedIn: 'root'
})
export class CephServiceService {
  private url = 'api/service';

  constructor(private http: HttpClient) {}

  list(serviceName?: string): Observable<CephServiceSpec[]> {
    const options = serviceName
      ? { params: new HttpParams().set('service_name', serviceName) }
      : {};
    return this.http.get<CephServiceSpec[]>(this.url, options);
  }

  getDaemons(serviceName?: string): Observable<Daemon[]> {
    return this.http.get<Daemon[]>(`${this.url}/${serviceName}/daemons`);
  }
}
