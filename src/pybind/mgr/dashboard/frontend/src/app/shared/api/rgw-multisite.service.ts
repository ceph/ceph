import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { RgwRealm, RgwZone, RgwZonegroup } from '~/app/ceph/rgw/models/rgw-multisite';
import { RgwDaemonService } from './rgw-daemon.service';
import { BehaviorSubject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class RgwMultisiteService {
  private uiUrl = 'ui-api/rgw/multisite';
  private url = 'api/rgw/multisite';

  private restartGatewayMessageSource = new BehaviorSubject<boolean>(null);
  restartGatewayMessage$ = this.restartGatewayMessageSource.asObservable();

  constructor(private http: HttpClient, public rgwDaemonService: RgwDaemonService) {}

  migrate(realm: RgwRealm, zonegroup: RgwZonegroup, zone: RgwZone, username: string) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        realm_name: realm.name,
        zonegroup_name: zonegroup.name,
        zone_name: zone.name,
        zonegroup_endpoints: zonegroup.endpoints,
        zone_endpoints: zone.endpoints,
        username: username
      });
      return this.http.put(`${this.uiUrl}/migrate`, null, { params: params });
    });
  }

  getSyncStatus() {
    return this.rgwDaemonService.request((params: HttpParams) => {
      return this.http.get(`${this.url}/sync_status`, { params: params });
    });
  }

  status() {
    return this.http.get(`${this.uiUrl}/status`);
  }

  getSyncPolicy(bucketName?: string, zonegroup?: string, fetchAllPolicy = false) {
    let params = new HttpParams();
    if (bucketName) {
      params = params.append('bucket_name', bucketName);
    }
    if (zonegroup) {
      params = params.append('zonegroup_name', zonegroup);
    }
    // fetchAllPolicy - if true, will fetch all the policy either linked or not linked with the buckets
    params = params.append('all_policy', fetchAllPolicy);
    return this.http.get(`${this.url}/sync-policy`, { params });
  }

  getSyncPolicyGroup(group_id: string, bucket_name?: string) {
    let params = new HttpParams();
    if (bucket_name) {
      params = params.append('bucket_name', bucket_name);
    }
    return this.http.get(`${this.url}/sync-policy-group/${group_id}`, { params });
  }

  createSyncPolicyGroup(payload: { group_id: string; status: string; bucket_name?: string }) {
    return this.http.post(`${this.url}/sync-policy-group`, payload);
  }

  modifySyncPolicyGroup(payload: { group_id: string; status: string; bucket_name?: string }) {
    return this.http.put(`${this.url}/sync-policy-group`, payload);
  }

  removeSyncPolicyGroup(group_id: string, bucket_name?: string) {
    let params = new HttpParams();
    if (bucket_name) {
      params = params.append('bucket_name', bucket_name);
    }
    return this.http.delete(`${this.url}/sync-policy-group/${group_id}`, { params });
  }

  setUpMultisiteReplication(
    realmName: string,
    zonegroupName: string,
    zonegroupEndpoints: string,
    zoneName: string,
    zoneEndpoints: string,
    username: string,
    cluster?: string,
    replicationZoneName?: string,
    clusterDetailsArray?: any
  ) {
    let params = new HttpParams()
      .set('realm_name', realmName)
      .set('zonegroup_name', zonegroupName)
      .set('zonegroup_endpoints', zonegroupEndpoints)
      .set('zone_name', zoneName)
      .set('zone_endpoints', zoneEndpoints)
      .set('username', username);

    if (cluster) {
      params = params.set('cluster_fsid', cluster);
    }

    if (clusterDetailsArray) {
      params = params.set('cluster_details', JSON.stringify(clusterDetailsArray));
    }

    if (replicationZoneName) {
      params = params.set('replication_zone_name', replicationZoneName);
    }

    return this.http.post(`${this.uiUrl}/multisite-replications`, null, { params: params });
  }

  createEditSyncFlow(payload: any) {
    return this.http.put(`${this.url}/sync-flow`, payload);
  }

  removeSyncFlow(flow_id: string, flow_type: string, group_id: string, bucket_name?: string) {
    let params = new HttpParams();
    if (bucket_name) {
      params = params.append('bucket_name', encodeURIComponent(bucket_name));
    }
    return this.http.delete(
      `${this.url}/sync-flow/${encodeURIComponent(flow_id)}/${flow_type}/${encodeURIComponent(
        group_id
      )}`,
      { params }
    );
  }

  createEditSyncPipe(payload: any, user?: string, mode?: string) {
    let params = new HttpParams();
    if (user) {
      params = params.append('user', user);
    }
    if (mode) {
      params = params.append('mode', mode);
    }
    return this.http.put(`${this.url}/sync-pipe`, payload, { params });
  }

  removeSyncPipe(pipe_id: string, group_id: string, bucket_name?: string) {
    let params = new HttpParams();
    if (bucket_name) {
      params = params.append('bucket_name', encodeURIComponent(bucket_name));
    }
    return this.http.delete(
      `${this.url}/sync-pipe/${encodeURIComponent(group_id)}/${encodeURIComponent(pipe_id)}`,
      { params }
    );
  }

  setRestartGatewayMessage(value: boolean): void {
    this.restartGatewayMessageSource.next(value);
  }
}
