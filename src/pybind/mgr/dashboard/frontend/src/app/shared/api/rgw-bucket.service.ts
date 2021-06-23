import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { of as observableOf } from 'rxjs';
import { catchError, mapTo } from 'rxjs/operators';

import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { cdEncode } from '~/app/shared/decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class RgwBucketService {
  private url = 'api/rgw/bucket';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {}

  /**
   * Get the list of buckets.
   * @return Observable<Object[]>
   */
  list() {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.append('stats', 'true');
      return this.http.get(this.url, { params: params });
    });
  }

  get(bucket: string) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.get(`${this.url}/${bucket}`, { params: params });
    });
  }

  create(
    bucket: string,
    uid: string,
    zonegroup: string,
    placementTarget: string,
    lockEnabled: boolean,
    lock_mode: 'GOVERNANCE' | 'COMPLIANCE',
    lock_retention_period_days: string
  ) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.post(this.url, null, {
        params: new HttpParams({
          fromObject: {
            bucket,
            uid,
            zonegroup,
            placement_target: placementTarget,
            lock_enabled: String(lockEnabled),
            lock_mode,
            lock_retention_period_days,
            daemon_name: params.get('daemon_name')
          }
        })
      });
    });
  }

  update(
    bucket: string,
    bucketId: string,
    uid: string,
    versioningState: string,
    mfaDelete: string,
    mfaTokenSerial: string,
    mfaTokenPin: string,
    lockMode: 'GOVERNANCE' | 'COMPLIANCE',
    lockRetentionPeriodDays: string
  ) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.append('bucket_id', bucketId);
      params = params.append('uid', uid);
      params = params.append('versioning_state', versioningState);
      params = params.append('mfa_delete', mfaDelete);
      params = params.append('mfa_token_serial', mfaTokenSerial);
      params = params.append('mfa_token_pin', mfaTokenPin);
      params = params.append('lock_mode', lockMode);
      params = params.append('lock_retention_period_days', lockRetentionPeriodDays);
      return this.http.put(`${this.url}/${bucket}`, null, { params: params });
    });
  }

  delete(bucket: string, purgeObjects = true) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.append('purge_objects', purgeObjects ? 'true' : 'false');
      return this.http.delete(`${this.url}/${bucket}`, { params: params });
    });
  }

  /**
   * Check if the specified bucket exists.
   * @param {string} bucket The bucket name to check.
   * @return Observable<boolean>
   */
  exists(bucket: string) {
    return this.get(bucket).pipe(
      mapTo(true),
      catchError((error: Event) => {
        if (_.isFunction(error.preventDefault)) {
          error.preventDefault();
        }
        return observableOf(false);
      })
    );
  }

  getLockDays(bucketData: object): number {
    if (bucketData['lock_retention_period_years'] > 0) {
      return Math.floor(bucketData['lock_retention_period_years'] * 365.242);
    }

    return bucketData['lock_retention_period_days'] || 0;
  }
}
