import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable()
export class TcmuIscsiService {

  constructor(private http: HttpClient) {
  }

  tcmuiscsi() {
    return this.http.get('api/tcmuiscsi').toPromise().then((resp: any) => {
      return resp;
    });
  }
}
