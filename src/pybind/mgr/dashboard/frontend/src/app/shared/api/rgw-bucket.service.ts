import { HttpClient, HttpParams, HttpResponse } from '@angular/common/http';
import { Injectable } from '@angular/core';
import 'rxjs/add/observable/forkJoin';
import 'rxjs/add/observable/of';
import { Observable } from 'rxjs/Observable';

import * as _ from 'lodash';

@Injectable()
export class RgwBucketService {

  private url = '/api/rgw/proxy/bucket';

  constructor(private http: HttpClient) { }

  list() {
    return this.http.get(this.url)
      .flatMap((buckets: string[]) => {
        if (buckets.length > 0) {
          return Observable.forkJoin(
            buckets.map((bucket: string) => {
              return this.get(bucket);
            })
          );
        }
        return Observable.of([]);
      });
  }

  get(bucket: string) {
    let params = new HttpParams();
    params = params.append('bucket', bucket);
    return this.http.get(this.url, { params: params });
  }

  create(bucket: string, uid: string) {
    const body = JSON.stringify({
      'bucket': bucket,
      'uid': uid
    });
    return this.http.post(`/api/rgw/bucket`, body);
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

  find(bucket: string) {
    let params = new HttpParams();
    params = params.append('bucket', bucket);
    return this.http.get(this.url, { params: params })
      .flatMap((resp: object | null) => {
        // Make sure we have received valid data.
        if ((null === resp) || (!_.isObjectLike(resp))) {
          return Observable.of([]);
        }
        // Return an array to be able to support wildcard searching someday.
        return Observable.of([resp]);
      });
  }
}
