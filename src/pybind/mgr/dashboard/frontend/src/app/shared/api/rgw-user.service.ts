import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { forkJoin as observableForkJoin, Observable, of as observableOf } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { cdEncode } from '../decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class RgwUserService {
  private url = 'api/rgw/user';

  constructor(private http: HttpClient) {}

  /**
   * Get the list of users.
   * @return {Observable<Object[]>}
   */
  list() {
    return this.enumerate().pipe(
      mergeMap((uids: string[]) => {
        if (uids.length > 0) {
          return observableForkJoin(
            uids.map((uid: string) => {
              return this.get(uid);
            })
          );
        }
        return observableOf([]);
      })
    );
  }

  /**
   * Get the list of usernames.
   * @return {Observable<string[]>}
   */
  enumerate() {
    return this.http.get(this.url);
  }

  enumerateEmail() {
    return this.http.get(`${this.url}/get_emails`);
  }

  get(uid: string) {
    return this.http.get(`${this.url}/${uid}`);
  }

  getQuota(uid: string) {
    return this.http.get(`${this.url}/${uid}/quota`);
  }

  create(args: Record<string, any>) {
    let params = new HttpParams();
    _.keys(args).forEach((key) => {
      params = params.append(key, args[key]);
    });
    return this.http.post(this.url, null, { params: params });
  }

  update(uid: string, args: Record<string, any>) {
    let params = new HttpParams();
    _.keys(args).forEach((key) => {
      params = params.append(key, args[key]);
    });
    return this.http.put(`${this.url}/${uid}`, null, { params: params });
  }

  updateQuota(uid: string, args: Record<string, string>) {
    let params = new HttpParams();
    _.keys(args).forEach((key) => {
      params = params.append(key, args[key]);
    });
    return this.http.put(`${this.url}/${uid}/quota`, null, { params: params });
  }

  delete(uid: string) {
    return this.http.delete(`${this.url}/${uid}`);
  }

  createSubuser(uid: string, args: Record<string, string>) {
    let params = new HttpParams();
    _.keys(args).forEach((key) => {
      params = params.append(key, args[key]);
    });
    return this.http.post(`${this.url}/${uid}/subuser`, null, { params: params });
  }

  deleteSubuser(uid: string, subuser: string) {
    return this.http.delete(`${this.url}/${uid}/subuser/${subuser}`);
  }

  addCapability(uid: string, type: string, perm: string) {
    let params = new HttpParams();
    params = params.append('type', type);
    params = params.append('perm', perm);
    return this.http.post(`${this.url}/${uid}/capability`, null, { params: params });
  }

  deleteCapability(uid: string, type: string, perm: string) {
    let params = new HttpParams();
    params = params.append('type', type);
    params = params.append('perm', perm);
    return this.http.delete(`${this.url}/${uid}/capability`, { params: params });
  }

  addS3Key(uid: string, args: Record<string, string>) {
    let params = new HttpParams();
    params = params.append('key_type', 's3');
    _.keys(args).forEach((key) => {
      params = params.append(key, args[key]);
    });
    return this.http.post(`${this.url}/${uid}/key`, null, { params: params });
  }

  deleteS3Key(uid: string, accessKey: string) {
    let params = new HttpParams();
    params = params.append('key_type', 's3');
    params = params.append('access_key', accessKey);
    return this.http.delete(`${this.url}/${uid}/key`, { params: params });
  }

  /**
   * Check if the specified user ID exists.
   * @param {string} uid The user ID to check.
   * @return {Observable<boolean>}
   */
  exists(uid: string): Observable<boolean> {
    return this.enumerate().pipe(
      mergeMap((resp: string[]) => {
        const index = _.indexOf(resp, uid);
        return observableOf(-1 !== index);
      })
    );
  }

  // Using @cdEncodeNot would be the preferred way here, but this
  // causes an error: https://tracker.ceph.com/issues/37505
  // Use decodeURIComponent as workaround.
  // emailExists(@cdEncodeNot email: string): Observable<boolean> {
  emailExists(email: string): Observable<boolean> {
    email = decodeURIComponent(email);
    return this.enumerateEmail().pipe(
      mergeMap((resp: any[]) => {
        const index = _.indexOf(resp, email);
        return observableOf(-1 !== index);
      })
    );
  }
}
