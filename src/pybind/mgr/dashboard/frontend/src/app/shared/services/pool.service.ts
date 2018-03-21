import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable()
export class PoolService {

  constructor(private http: HttpClient) {
  }

  list(attrs = []) {
    const attrsStr = attrs.join(',');
    return this.http.get(`api/pool?attrs=${attrsStr}`).toPromise().then((resp: any) => {
      return resp;
    });
  }
}
