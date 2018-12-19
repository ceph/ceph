import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';

import { ErasureCodeProfile } from '../models/erasure-code-profile';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class ErasureCodeProfileService {
  apiPath = 'api/erasure_code_profile';

  constructor(private http: HttpClient) {}

  list(): Observable<ErasureCodeProfile[]> {
    return this.http.get<ErasureCodeProfile[]>(this.apiPath);
  }

  create(ecp: ErasureCodeProfile) {
    return this.http.post(this.apiPath, ecp, { observe: 'response' });
  }

  update(ecp: ErasureCodeProfile) {
    return this.http.put(`${this.apiPath}/${ecp.name}`, ecp, { observe: 'response' });
  }

  delete(name: string) {
    return this.http.delete(`${this.apiPath}/${name}`, { observe: 'response' });
  }

  get(name: string) {
    return this.http.get(`${this.apiPath}/${name}`);
  }

  getInfo() {
    return this.http.get(`${this.apiPath}/_info`);
  }
}
