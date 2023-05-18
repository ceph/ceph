import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { RgwRealm, RgwZone, RgwZonegroup } from '~/app/ceph/rgw/models/rgw-multisite';

@Injectable({
  providedIn: 'root'
})
export class RgwMultisiteService {
  private url = 'ui-api/rgw/multisite';

  constructor(private http: HttpClient) {}

  migrate(realm: RgwRealm, zonegroup: RgwZonegroup, zone: RgwZone, user: string) {
    let requestBody = {
      realm_name: realm.name,
      zonegroup_name: zonegroup.name,
      zone_name: zone.name,
      zonegroup_endpoints: zonegroup.endpoints,
      zone_endpoints: zone.endpoints,
      user: user
    };
    return this.http.put(`${this.url}/migrate`, requestBody);
  }
}
