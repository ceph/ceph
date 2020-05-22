import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { cdEncode } from '../decorators/cd-encode';
import { ApiModule } from './api.module';

@cdEncode
@Injectable({
  providedIn: ApiModule
})
export class RgwSiteService {
  private url = 'api/rgw/site';

  constructor(private http: HttpClient) {}

  get(query?: string) {
    let params = new HttpParams();
    if (query) {
      params = params.append('query', query);
    }

    return this.http.get(this.url, { params: params });
  }
}
