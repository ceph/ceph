import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { forkJoin as observableForkJoin, of as observableOf } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class RgwBucketService {
  private url = '/api/rgw/proxy/bucket';

  constructor(private http: HttpClient) {}

  /**
   * Get the list of buckets.
   * @return {Observable<Object[]>}
   */
  list() {
    return this.enumerate().pipe(
      mergeMap((buckets: string[]) => {
        if (buckets.length > 0) {
          return observableForkJoin(
            buckets.map((bucket: string) => {
              return this.get(bucket);
            }));
        }
        return observableOf([]);
      }));
  }

  /**
   * Get the list of bucket names.
   * @return {Observable<string[]>}
   */
  enumerate() {
    return this.http.get(this.url);
  }

  get(bucket: string) {
    let params = new HttpParams();
    params = params.append('bucket', bucket);
    return this.http.get(this.url, { params: params });
  }

  create(bucket: string, uid: string) {
    const body = {
      bucket: bucket,
      uid: uid
    };
    return this.http.post('/api/rgw/bucket', body);
  }

  update(bucketId: string, bucket: string, uid: string) {
    let params = new HttpParams();
    params = params.append('bucket', bucket);
    params = params.append('bucket-id', bucketId as string);
    params = params.append('uid', uid);
    return this.http.put(this.url, null, { params: params });
  }

  delete(bucket: string, purgeObjects = true) {
    let params = new HttpParams();
    params = params.append('bucket', bucket);
    params = params.append('purge-objects', purgeObjects ? 'true' : 'false');
    return this.http.delete(this.url, { params: params });
  }

  /**
   * Check if the specified bucket exists.
   * @param {string} uid The bucket name to check.
   * @return {Observable<boolean>}
   */
  exists(bucket: string) {
    return this.enumerate().pipe(
      mergeMap((resp: string[]) => {
        const index = _.indexOf(resp, bucket);
        return observableOf(-1 !== index);
      }));
  }
}
