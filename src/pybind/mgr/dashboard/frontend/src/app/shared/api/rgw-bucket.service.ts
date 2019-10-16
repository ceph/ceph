import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { forkJoin as observableForkJoin, of as observableOf } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { cdEncode } from '../decorators/cd-encode';
import { ApiModule } from './api.module';

@cdEncode
@Injectable({
  providedIn: ApiModule
})
export class RgwBucketService {
  private url = 'api/rgw/bucket';

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
            })
          );
        }
        return observableOf([]);
      })
    );
  }

  /**
   * Get the list of bucket names.
   * @return {Observable<string[]>}
   */
  enumerate() {
    return this.http.get(this.url);
  }

  get(bucket: string) {
    return this.http.get(`${this.url}/${bucket}`);
  }

  create(bucket: string, uid: string, zonegroup: string, placementTarget: string) {
    let params = new HttpParams();
    params = params.append('bucket', bucket);
    params = params.append('uid', uid);
    params = params.append('zonegroup', zonegroup);
    params = params.append('placement_target', placementTarget);

    return this.http.post(this.url, null, { params: params });
  }

  update(bucket: string, bucketId: string, uid: string, versioningState: string) {
    let params = new HttpParams();
    params = params.append('bucket_id', bucketId);
    params = params.append('uid', uid);
    params = params.append('versioning_state', versioningState);
    return this.http.put(`${this.url}/${bucket}`, null, { params: params });
  }

  delete(bucket: string, purgeObjects = true) {
    let params = new HttpParams();
    params = params.append('purge_objects', purgeObjects ? 'true' : 'false');
    return this.http.delete(`${this.url}/${bucket}`, { params: params });
  }

  /**
   * Check if the specified bucket exists.
   * @param {string} bucket The bucket name to check.
   * @return {Observable<boolean>}
   */
  exists(bucket: string) {
    return this.enumerate().pipe(
      mergeMap((resp: string[]) => {
        const index = _.indexOf(resp, bucket);
        return observableOf(-1 !== index);
      })
    );
  }
}
