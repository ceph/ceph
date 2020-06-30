import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { cdEncode } from '../decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class RgwDaemonService {
  private url = 'api/rgw/daemon';

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(this.url);
  }

  get(id: string) {
    return this.http.get(`${this.url}/${id}`);
  }
}
