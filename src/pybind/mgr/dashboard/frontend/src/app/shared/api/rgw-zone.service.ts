import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { RgwRealm, RgwZone, RgwZonegroup } from '~/app/ceph/rgw/models/rgw-multisite';
import { Icons } from '../enum/icons.enum';
import { RgwDaemonService } from './rgw-daemon.service';

@Injectable({
  providedIn: 'root'
})
export class RgwZoneService {
  private url = 'api/rgw/zone';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {}

  create(
    zone: RgwZone,
    zonegroup: RgwZonegroup,
    defaultZone: boolean,
    master: boolean,
    endpoints: Array<string>,
    user: string
  ) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        zone_name: zone.name,
        zonegroup_name: zonegroup.name,
        default: defaultZone,
        master: master,
        zone_endpoints: endpoints,
        user: user
      });
      return this.http.post(`${this.url}`, null, { params: params });
    });
  }

  list(): Observable<object> {
    return this.rgwDaemonService.request(() => {
      return this.http.get<object>(`${this.url}`);
    });
  }

  get(zone: RgwZone): Observable<RgwZone> {
    return this.rgwDaemonService.request(() => {
      return this.http.get(`${this.url}/${zone.name}`);
    });
  }

  getAllZonesInfo(): Observable<object> {
    return this.rgwDaemonService.request(() => {
      return this.http.get(`${this.url}/get_all_zones_info`);
    });
  }

  getZoneTree(zone: RgwZone, defaultZoneId: string, zonegroup?: RgwZonegroup, realm?: RgwRealm) {
    let nodes = {};
    let zoneIds = [];
    nodes['id'] = zone.id;
    zoneIds.push(zone.id);
    nodes['name'] = zone.name + ' (zone)';
    nodes['info'] = zone;
    nodes['icon'] = Icons.deploy;
    nodes['parent'] = zonegroup ? zonegroup.name : '';
    nodes['second_parent'] = realm ? realm.name : '';
    nodes['is_default'] = zone.id === defaultZoneId ? true : false;
    nodes['is_master'] = zonegroup && zonegroup.master_zone === zone.id ? true : false;
    return {
      nodes: nodes,
      zoneIds: zoneIds
    };
  }
}
