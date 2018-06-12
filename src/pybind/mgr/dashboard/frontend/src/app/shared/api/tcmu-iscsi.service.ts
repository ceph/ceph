import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class TcmuIscsiService {

  constructor(private http: HttpClient) {
  }

  tcmuiscsi() {
    return this.http.get('api/tcmuiscsi').toPromise().then((resp: any) => {
      return resp;
    });
  }
}
