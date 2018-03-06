import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable()
export class HostService {

  constructor(private http: HttpClient) {
  }

  list() {
    return this.http.get('api/host').toPromise().then((resp: any) => {
      return resp;
    });
  }
}
