import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import 'rxjs/add/observable/forkJoin';
import 'rxjs/add/observable/of';
import { Observable } from 'rxjs/Observable';

@Injectable()
export class RgwUserService {

  private url = '/api/rgw/proxy/user';

  constructor(private http: HttpClient) { }

  list() {
    return this.enumerate()
      .flatMap((uids: string[]) => {
        if (uids.length > 0) {
          return Observable.forkJoin(
            uids.map((uid: string) => {
              return this.get(uid);
            })
          );
        }
        return Observable.of([]);
      });
  }

  enumerate() {
    return this.http.get('/api/rgw/proxy/metadata/user');
  }

  get(uid: string) {
    let params = new HttpParams();
    params = params.append('uid', uid);
    return this.http.get(this.url, { params: params });
  }

  getQuota(uid: string) {
    let params = new HttpParams();
    params = params.append('uid', uid);
    return this.http.get(`${this.url}?quota`, {params: params});
  }
}
