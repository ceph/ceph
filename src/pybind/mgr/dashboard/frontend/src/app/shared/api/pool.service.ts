import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { cdEncode } from '../decorators/cd-encode';
import { ApiModule } from './api.module';

@cdEncode
@Injectable({
  providedIn: ApiModule
})
export class PoolService {
  apiPath = 'api/pool';

  constructor(private http: HttpClient) {}

  create(pool) {
    return this.http.post(this.apiPath, pool, { observe: 'response' });
  }

  update(pool) {
    const name = pool.pool;
    delete pool.pool;
    return this.http.put(`${this.apiPath}/${name}`, pool, { observe: 'response' });
  }

  delete(name) {
    return this.http.delete(`${this.apiPath}/${name}`, { observe: 'response' });
  }

  get(poolName) {
    return this.http.get(`${this.apiPath}/${poolName}`);
  }

  getList() {
    return this.http.get(this.apiPath);
  }

  getInfo() {
    return this.http.get(`${this.apiPath}/_info`);
  }

  list(attrs = []) {
    const attrsStr = attrs.join(',');
    return this.http
      .get(`${this.apiPath}?attrs=${attrsStr}`)
      .toPromise()
      .then((resp: any) => {
        return resp;
      });
  }
}
