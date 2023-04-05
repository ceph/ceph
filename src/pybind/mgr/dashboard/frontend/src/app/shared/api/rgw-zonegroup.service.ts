import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { RgwRealm, RgwZonegroup } from '~/app/ceph/rgw/models/rgw-multisite';
import { Icons } from '../enum/icons.enum';
import { RgwDaemonService } from './rgw-daemon.service';

@Injectable({
  providedIn: 'root'
})
export class RgwZonegroupService {
  private url = 'api/rgw/zonegroup';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {}

  create(realm: RgwRealm, zonegroup: RgwZonegroup, defaultZonegroup: boolean, master: boolean) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        realm_name: realm.name,
        zonegroup_name: zonegroup.name,
        default: defaultZonegroup,
        master: master,
        zonegroup_endpoints: zonegroup.endpoints
      });
      return this.http.post(`${this.url}`, null, { params: params });
    });
  }

  update(
    realm: RgwRealm,
    zonegroup: RgwZonegroup,
    newZonegroupName: string,
    defaultZonegroup: boolean,
    master: boolean,
    removedZones: string[],
    addedZones: string[]
  ) {
    return this.rgwDaemonService.request((requestBody: any) => {
      requestBody = {
        zonegroup_name: zonegroup.name,
        realm_name: realm.name,
        new_zonegroup_name: newZonegroupName,
        default: defaultZonegroup,
        master: master,
        zonegroup_endpoints: zonegroup.endpoints,
        placement_targets: zonegroup.placement_targets,
        remove_zones: removedZones,
        add_zones: addedZones
      };
      return this.http.put(`${this.url}/${zonegroup.name}`, requestBody);
    });
  }

  list(): Observable<object> {
    return this.rgwDaemonService.request(() => {
      return this.http.get<object>(`${this.url}`);
    });
  }

  get(zonegroup: RgwZonegroup): Observable<RgwZonegroup> {
    return this.rgwDaemonService.request(() => {
      return this.http.get(`${this.url}/${zonegroup.name}`);
    });
  }

  getAllZonegroupsInfo(): Observable<object> {
    return this.rgwDaemonService.request(() => {
      return this.http.get(`${this.url}/get_all_zonegroups_info`);
    });
  }

  delete(zonegroupName: string, deletePools: boolean): Observable<any> {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        zonegroup_name: zonegroupName,
        delete_pools: deletePools
      });
      return this.http.delete(`${this.url}/${zonegroupName}`, { params: params });
    });
  }

  getZonegroupTree(zonegroup: RgwZonegroup, defaultZonegroupId: string, realm?: RgwRealm) {
    let nodes = {};
    nodes['id'] = zonegroup.id;
    nodes['name'] = zonegroup.name;
    nodes['info'] = zonegroup;
    nodes['icon'] = Icons.cubes;
    nodes['is_master'] = zonegroup.is_master;
    nodes['parent'] = realm ? realm.name : '';
    nodes['is_default'] = zonegroup.id === defaultZonegroupId ? true : false;
    nodes['type'] = 'zonegroup';
    nodes['endpoints'] = zonegroup.endpoints;
    nodes['master_zone'] = zonegroup.master_zone;
    nodes['zones'] = zonegroup.zones;
    nodes['placement_targets'] = zonegroup.placement_targets;
    return nodes;
  }
}
