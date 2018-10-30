import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { cdEncode } from '../decorators/cd-encode';

@cdEncode
@Injectable()
export class PoolService {
  constructor(private http: HttpClient) {}

  getList() {
    return this.http.get('api/pool');
  }

  list(attrs = []) {
    const attrsStr = attrs.join(',');
    return this.http
      .get(`api/pool?attrs=${attrsStr}`)
      .toPromise()
      .then((resp: any) => {
        return resp;
      });
  }
}
