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
    user: string,
    createSystemUser: boolean,
    master_zone_of_master_zonegroup: RgwZone
  ) {
    let master_zone_name = '';
    if (master_zone_of_master_zonegroup !== undefined) {
      master_zone_name = master_zone_of_master_zonegroup.name;
    } else {
      master_zone_name = '';
    }
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        zone_name: zone.name,
        zonegroup_name: zonegroup.name,
        default: defaultZone,
        master: master,
        zone_endpoints: endpoints,
        user: user,
        createSystemUser: createSystemUser,
        master_zone_of_master_zonegroup: master_zone_name
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

  delete(zonegroupName: string, zoneName: string, deletePools: boolean): Observable<any> {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        zonegroup_name: zonegroupName,
        zone_name: zoneName,
        delete_pools: deletePools
      });
      return this.http.delete(`${this.url}/${zoneName}`, { params: params });
    });
  }

  update(
    zone: RgwZone,
    zonegroup: RgwZonegroup,
    newZoneName: string,
    defaultZone?: boolean,
    master?: boolean,
    endpoints?: Array<string>,
    user?: string,
    placementTarget?: string,
    dataPool?: string,
    indexPool?: string,
    dataExtraPool?: string,
    storageClass?: string,
    dataPoolClass?: string,
    compression?: string,
    master_zone_of_master_zonegroup?: RgwZone
  ) {
    let master_zone_name = '';
    if (master_zone_of_master_zonegroup !== undefined) {
      master_zone_name = master_zone_of_master_zonegroup.name;
    } else {
      master_zone_name = '';
    }
    return this.rgwDaemonService.request((requestBody: any) => {
      requestBody = {
        zone_name: zone.name,
        zonegroup_name: zonegroup.name,
        new_zone_name: newZoneName,
        default: defaultZone,
        master: master,
        zone_endpoints: endpoints,
        user: user,
        placement_target: placementTarget,
        data_pool: dataPool,
        index_pool: indexPool,
        data_extra_pool: dataExtraPool,
        storage_class: storageClass,
        data_pool_class: dataPoolClass,
        compression: compression,
        master_zone_of_master_zonegroup: master_zone_name
      };
      return this.http.put(`${this.url}/${zone.name}`, requestBody);
    });
  }

  getZoneTree(zone: RgwZone, defaultZoneId: string, zonegroup?: RgwZonegroup, realm?: RgwRealm) {
    let nodes = {};
    let zoneIds = [];
    nodes['id'] = zone.id;
    zoneIds.push(zone.id);
    nodes['name'] = zone.name;
    nodes['type'] = 'zone';
    nodes['name'] = zone.name;
    nodes['info'] = zone;
    nodes['icon'] = Icons.deploy;
    nodes['zone_zonegroup'] = zonegroup;
    nodes['parent'] = zonegroup ? zonegroup.name : '';
    nodes['second_parent'] = realm ? realm.name : '';
    nodes['is_default'] = zone.id === defaultZoneId ? true : false;
    nodes['endpoints'] = zone.endpoints;
    nodes['is_master'] = zonegroup && zonegroup.master_zone === zone.id ? true : false;
    nodes['type'] = 'zone';
    return {
      nodes: nodes,
      zoneIds: zoneIds
    };
  }

  getPoolNames() {
    return this.rgwDaemonService.request(() => {
      return this.http.get(`${this.url}/get_pool_names`);
    });
  }

  createSystemUser(userName: string, zone: string) {
    return this.rgwDaemonService.request((requestBody: any) => {
      requestBody = {
        userName: userName,
        zoneName: zone
      };
      return this.http.put(`${this.url}/create_system_user`, requestBody);
    });
  }

  getUserList(zoneName: string) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        zoneName: zoneName
      });
      return this.http.get(`${this.url}/get_user_list`, { params: params });
    });
  }
}
