import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import 'rxjs/add/observable/forkJoin';
import 'rxjs/add/observable/of';
import { Observable } from 'rxjs/Observable';

import { cdEncode } from '../decorators/cd-encode';

@cdEncode
@Injectable()
export class RgwUserService {

  private url = 'api/rgw/proxy/user';

  constructor(private http: HttpClient) { }

  /**
   * Get the list of users.
   * @return {Observable<Object[]>}
   */
  list() {
    return this.enumerate()
      .flatMap((uids: string[]) => {
        if (uids.length > 0) {
          return Observable.forkJoin(
            uids.map((uid: string) => {
              return this.get(uid);
            }));
        }
        return Observable.of([]);
      });
  }

  /**
   * Get the list of usernames.
   * @return {Observable<string[]>}
   */
  enumerate() {
    return this.http.get('api/rgw/proxy/metadata/user');
  }

  get(uid: string) {
    let params = new HttpParams();
    params = params.append('uid', uid);
    return this.http.get(this.url, {params: params});
  }

  getQuota(uid: string) {
    let params = new HttpParams();
    params = params.append('uid', uid);
    return this.http.get(`${this.url}?quota`, {params: params});
  }

  put(args: object) {
    let params = new HttpParams();
    _.keys(args).forEach((key) => {
      params = params.append(key, args[key]);
    });
    return this.http.put(this.url, null, {params: params});
  }

  putQuota(args: object) {
    let params = new HttpParams();
    _.keys(args).forEach((key) => {
      params = params.append(key, args[key]);
    });
    return this.http.put(`${this.url}?quota`, null, {params: params});
  }

  post(args: object) {
    let params = new HttpParams();
    _.keys(args).forEach((key) => {
      params = params.append(key, args[key]);
    });
    return this.http.post(this.url, null, {params: params});
  }

  delete(uid: string) {
    return this.http.delete(`api/rgw/user/${uid}`);
  }

  addSubuser(uid: string, subuser: string, permissions: string,
             secretKey: string, generateSecret: boolean) {
    const mapPermissions = {
      'full-control': 'full',
      'read-write': 'readwrite'
    };
    let params = new HttpParams();
    params = params.append('uid', uid);
    params = params.append('subuser', subuser);
    params = params.append('key-type', 'swift');
    params = params.append('access', (permissions in mapPermissions) ?
      mapPermissions[permissions] : permissions);
    if (generateSecret) {
      params = params.append('generate-secret', 'true');
    } else {
      params = params.append('secret-key', secretKey);
    }
    return this.http.put(this.url, null, {params: params});
  }

  deleteSubuser(uid: string, subuser: string) {
    let params = new HttpParams();
    params = params.append('uid', uid);
    params = params.append('subuser', subuser);
    params = params.append('purge-keys', 'true');
    return this.http.delete(this.url, {params: params});
  }

  addCapability(uid: string, type: string, perm: string) {
    let params = new HttpParams();
    params = params.append('uid', uid);
    params = params.append('user-caps', `${type}=${perm}`);
    return this.http.put(`${this.url}?caps`, null, {params: params});
  }

  deleteCapability(uid: string, type: string, perm: string) {
    let params = new HttpParams();
    params = params.append('uid', uid);
    params = params.append('user-caps', `${type}=${perm}`);
    return this.http.delete(`${this.url}?caps`, {params: params});
  }

  addS3Key(uid: string, subuser: string, accessKey: string,
           secretKey: string, generateKey: boolean) {
    let params = new HttpParams();
    params = params.append('uid', uid);
    params = params.append('key-type', 's3');
    params = params.append('generate-key', generateKey ? 'true' : 'false');
    if (!generateKey) {
      params = params.append('access-key', accessKey);
      params = params.append('secret-key', secretKey);
    }
    if (!_.isEmpty(subuser)) {
      params = params.append('subuser', subuser);
    }
    return this.http.put(`${this.url}?key`, null, {params: params});
  }

  deleteS3Key(uid: string, accessKey: string) {
    let params = new HttpParams();
    params = params.append('uid', uid);
    params = params.append('key-type', 's3');
    params = params.append('access-key', accessKey);
    return this.http.delete(`${this.url}?key`, {params: params});
  }

  /**
   * Check if the specified user ID exists.
   * @param {string} uid The user ID to check.
   * @return {Observable<boolean>}
   */
  exists(uid: string) {
    return this.enumerate()
      .flatMap((resp: string[]) => {
        const index = _.indexOf(resp, uid);
        return Observable.of(-1 !== index);
      });
  }
}
