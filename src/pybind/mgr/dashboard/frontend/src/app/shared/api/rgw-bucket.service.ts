import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { BehaviorSubject, of as observableOf } from 'rxjs';
import { catchError, map, mapTo } from 'rxjs/operators';
import { Bucket } from '~/app/ceph/rgw/models/rgw-bucket';

import { ApiClient } from '~/app/shared/api/api-client';
import { RgwDaemonService } from '~/app/shared/api/rgw-daemon.service';
import { cdEncode } from '~/app/shared/decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class RgwBucketService extends ApiClient {
  private url = 'api/rgw/bucket';
  private bucketsSubject = new BehaviorSubject<Bucket[]>([]);
  private totalNumObjectsSubject = new BehaviorSubject<number>(0);
  private totalUsedCapacitySubject = new BehaviorSubject<number>(0);
  private averageObjectSizeSubject = new BehaviorSubject<number>(0);
  buckets$ = this.bucketsSubject.asObservable();
  totalNumObjects$ = this.totalNumObjectsSubject.asObservable();
  totalUsedCapacity$ = this.totalUsedCapacitySubject.asObservable();
  averageObjectSize$ = this.averageObjectSizeSubject.asObservable();

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {
    super();
  }

  fetchAndTransformBuckets() {
    return this.list(true).pipe(
      map((buckets: Bucket[]) => {
        let totalNumObjects = 0;
        let totalUsedCapacity = 0;
        let averageObjectSize = 0;
        const transformedBuckets = buckets.map((bucket) => this.transformBucket(bucket));
        transformedBuckets.forEach((bucket) => {
          totalNumObjects += bucket?.num_objects || 0;
          totalUsedCapacity += bucket?.bucket_size || 0;
        });
        averageObjectSize = this.calculateAverageObjectSize(totalNumObjects, totalUsedCapacity);
        this.bucketsSubject.next(transformedBuckets);
        this.totalNumObjectsSubject.next(totalNumObjects);
        this.totalUsedCapacitySubject.next(totalUsedCapacity);
        this.averageObjectSizeSubject.next(averageObjectSize);
      })
    );
  }

  transformBucket(bucket: Bucket) {
    const maxBucketSize = bucket?.bucket_quota?.max_size ?? 0;
    const maxBucketObjects = bucket?.bucket_quota?.max_objects ?? 0;
    const bucket_size = bucket['usage']?.['rgw.main']?.['size_actual'] || 0;
    const num_objects = bucket['usage']?.['rgw.main']?.['num_objects'] || 0;
    return {
      ...bucket,
      bucket_size,
      num_objects,
      size_usage: this.calculateSizeUsage(bucket_size, maxBucketSize),
      object_usage: this.calculateObjectUsage(num_objects, maxBucketObjects)
    };
  }

  calculateSizeUsage(bucket_size: number, maxBucketSize: number) {
    return maxBucketSize > 0 ? bucket_size / maxBucketSize : undefined;
  }

  calculateObjectUsage(num_objects: number, maxBucketObjects: number) {
    return maxBucketObjects > 0 ? num_objects / maxBucketObjects : undefined;
  }

  calculateAverageObjectSize(totalNumObjects: number, totalUsedCapacity: number) {
    return totalNumObjects > 0 ? totalUsedCapacity / totalNumObjects : 0;
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
    cannedAcl: string,
    replication: string
  ) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      const paramsObject = {
        bucket,
        uid,
        zonegroup,
        lock_enabled: String(lockEnabled),
        lock_mode,
        lock_retention_period_days,
        encryption_state: String(encryption_state),
        encryption_type,
        key_id,
        tags: tags,
        bucket_policy: bucketPolicy,
        canned_acl: cannedAcl,
        replication: replication,
        daemon_name: params.get('daemon_name')
      };

      if (placementTarget) {
        paramsObject['placement_target'] = placementTarget;
      }

      return this.http.post(this.url, null, {
        params: new HttpParams({ fromObject: paramsObject })
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
    cannedAcl: string,
    replication: string,
    lifecycle: string
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
        canned_acl: cannedAcl,
        replication: replication,
        lifecycle: lifecycle
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
