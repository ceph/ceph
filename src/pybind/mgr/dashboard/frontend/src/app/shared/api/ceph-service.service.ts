import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { ApiClient } from '~/app/shared/api/api-client';
import { Daemon } from '../models/daemon.interface';
import { CephServiceSpec } from '../models/service.interface';
import { PaginateObservable } from './paginate.model';

@Injectable({
  providedIn: 'root'
})
export class CephServiceService extends ApiClient {
  private url = 'api/service';

  constructor(private http: HttpClient) {
    super();
  }

  list(httpParams: HttpParams, serviceName?: string): PaginateObservable<CephServiceSpec[]> {
    const options = {
      headers: { Accept: this.getVersionHeaderValue(2, 0) },
      params: httpParams
    };
    options['observe'] = 'response';
    if (serviceName) {
      options.params = options.params.append('service_name', serviceName);
    }
    return new PaginateObservable<CephServiceSpec[]>(
      this.http.get<CephServiceSpec[]>(this.url, options)
    );
  }

  getDaemons(serviceName?: string): Observable<Daemon[]> {
    return this.http.get<Daemon[]>(`${this.url}/${serviceName}/daemons`);
  }

  create(serviceSpec: { [key: string]: any }) {
    const serviceName = serviceSpec['service_id']
      ? `${serviceSpec['service_type']}.${serviceSpec['service_id']}`
      : serviceSpec['service_type'];
    return this.http.post(
      this.url,
      {
        service_name: serviceName,
        service_spec: serviceSpec
      },
      { observe: 'response' }
    );
  }

  update(serviceSpec: { [key: string]: any }) {
    const serviceName = serviceSpec['service_id']
      ? `${serviceSpec['service_type']}.${serviceSpec['service_id']}`
      : serviceSpec['service_type'];
    return this.http.put(
      `${this.url}/${serviceName}`,
      {
        service_name: serviceName,
        service_spec: serviceSpec
      },
      { observe: 'response' }
    );
  }

  delete(serviceName: string) {
    return this.http.delete(`${this.url}/${serviceName}`, { observe: 'response' });
  }

  getKnownTypes(): Observable<string[]> {
    return this.http.get<string[]>(`${this.url}/known_types`);
  }
}
