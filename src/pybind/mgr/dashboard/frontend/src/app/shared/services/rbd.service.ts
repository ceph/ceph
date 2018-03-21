import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable()
export class RbdService {

  constructor(private http: HttpClient) {
  }

  create(rbd) {
    return this.http.post('/api/rbd', rbd).toPromise().then((resp: any) => {
      return resp;
    });
  }

  getPoolImages(pool) {
    return this.http.get(`api/rbd/${pool}`).toPromise().then((resp: any) => {
      return resp;
    });
  }
}
