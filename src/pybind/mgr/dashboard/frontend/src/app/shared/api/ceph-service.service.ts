import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { Daemon } from '../models/daemon.interface';
import { CephService } from '../models/service.interface';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class CephServiceService {
  private url = 'api/service';

  constructor(private http: HttpClient) {}

  list(serviceName?: string): Observable<CephService[]> {
    const options = serviceName
      ? { params: new HttpParams().set('service_name', serviceName) }
      : {};
    return this.http.get<CephService[]>(this.url, options);
  }

  getDaemons(serviceName?: string): Observable<Daemon[]> {
    return this.http.get<Daemon[]>(`${this.url}/${serviceName}/daemons`);
  }
}
