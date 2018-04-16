import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable()
export class PoolService {

  constructor(private http: HttpClient) {
  }

  rbdPoolImages(pool) {
    return this.http.get(`api/block/image?pool_name=${pool}`).toPromise().then((resp: any) => {
      return resp;
    });
  }
}
