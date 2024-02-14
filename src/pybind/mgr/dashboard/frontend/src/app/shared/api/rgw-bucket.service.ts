import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { of as observableOf } from 'rxjs';
import { catchError, mapTo } from 'rxjs/operators';

import { ApiClient } from '~/app/shared/api/api-client';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { cdEncode } from '~/app/shared/decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class RgwBucketService extends ApiClient {
  private url = 'api/rgw/bucket';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {
    super();
  }

  /**
   * Get the list of buckets.
   * @return Observable<Object[]>
   */
  list(stats: boolean = false, uid: string = '') {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.append('stats', stats.toString());
      if (uid) {
        params = params.append('uid', uid);
      }
      return this.http.get(this.url, {
        headers: { Accept: this.getVersionHeaderValue(1, 1) },
        params: params
      });
    });
  }

  get(bucket: string) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.get(`${this.url}/${bucket}`, { params: params });
    });
  }

  getTotalBucketsAndUsersLength() {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.get(`ui-${this.url}/buckets_and_users_count`, { params: params });
    });
  }

  create(
    bucket: string,
    uid: string,
    zonegroup: string,
    placementTarget: string,
    lockEnabled: boolean,
    lock_mode: 'GOVERNANCE' | 'COMPLIANCE',
    lock_retention_period_days: string,
    encryption_state: boolean,
    encryption_type: string,
    key_id: string,
    tags: string,
    bucketPolicy: string,
    cannedAcl: string
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
            encryption_state: String(encryption_state),
            encryption_type,
            key_id,
            tags: tags,
            bucket_policy: bucketPolicy,
            canned_acl: cannedAcl,
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
    encryptionState: boolean,
    encryptionType: string,
    keyId: string,
    mfaDelete: string,
    mfaTokenSerial: string,
    mfaTokenPin: string,
    lockMode: 'GOVERNANCE' | 'COMPLIANCE',
    lockRetentionPeriodDays: string,
    tags: string,
    bucketPolicy: string,
    cannedAcl: string
  ) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        bucket_id: bucketId,
        uid: uid,
        versioning_state: versioningState,
        encryption_state: String(encryptionState),
        encryption_type: encryptionType,
        key_id: keyId,
        mfa_delete: mfaDelete,
        mfa_token_serial: mfaTokenSerial,
        mfa_token_pin: mfaTokenPin,
        lock_mode: lockMode,
        lock_retention_period_days: lockRetentionPeriodDays,
        tags: tags,
        bucket_policy: bucketPolicy,
        canned_acl: cannedAcl
      });
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

  setEncryptionConfig(
    encryption_type: string,
    kms_provider: string,
    auth_method: string,
    secret_engine: string,
    secret_path: string,
    namespace: string,
    address: string,
    token: string,
    owner: string,
    ssl_cert: string,
    client_cert: string,
    client_key: string
  ) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        encryption_type: encryption_type,
        kms_provider: kms_provider,
        auth_method: auth_method,
        secret_engine: secret_engine,
        secret_path: secret_path,
        namespace: namespace,
        address: address,
        token: token,
        owner: owner,
        ssl_cert: ssl_cert,
        client_cert: client_cert,
        client_key: client_key
      });
      return this.http.put(`${this.url}/setEncryptionConfig`, null, { params: params });
    });
  }

  getEncryption(bucket: string) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.get(`${this.url}/${bucket}/getEncryption`, { params: params });
    });
  }

  deleteEncryption(bucket: string) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.get(`${this.url}/${bucket}/deleteEncryption`, { params: params });
    });
  }

  getEncryptionConfig() {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.get(`${this.url}/getEncryptionConfig`, { params: params });
    });
  }
}
