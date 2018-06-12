import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class HostService {

  constructor(private http: HttpClient) {
  }

  list() {
    return this.http.get('api/host').toPromise().then((resp: any) => {
      return resp;
    });
  }
}
