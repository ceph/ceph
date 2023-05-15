import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { RgwDaemonService } from './rgw-daemon.service';
import { RgwRealm, RgwZone, RgwZonegroup } from '~/app/ceph/rgw/models/rgw-multisite';

@Injectable({
  providedIn: 'root'
})
export class RgwMultisiteService {
  private url = 'ui-api/rgw/multisite';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {}

  getMultisiteSyncStatus() {
    return this.rgwDaemonService.request(() => {
      return this.http.get(`${this.url}/sync_status`);
    });
  }

  migrate(realm: RgwRealm, zonegroup: RgwZonegroup, zone: RgwZone, user: string) {
    return this.rgwDaemonService.request((requestBody: any) => {
      requestBody = {
        realm_name: realm.name,
        zonegroup_name: zonegroup.name,
        zone_name: zone.name,
        zonegroup_endpoints: zonegroup.endpoints,
        zone_endpoints: zone.endpoints,
        user: user
      };
      return this.http.put(`${this.url}/migrate`, requestBody);
    });
  }
}
