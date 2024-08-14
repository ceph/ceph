import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { RgwRealm, RgwZone, RgwZonegroup } from '~/app/ceph/rgw/models/rgw-multisite';
import { Icons } from '../enum/icons.enum';

@Injectable({
  providedIn: 'root'
})
export class RgwZoneService {
  private url = 'api/rgw/zone';

  constructor(private http: HttpClient) {}

  create(
    zone: RgwZone,
    zonegroup: RgwZonegroup,
    defaultZone: boolean,
    master: boolean,
    endpoints: string
  ) {
    let params = new HttpParams();
    params = params.appendAll({
      zone_name: zone.name,
      zonegroup_name: zonegroup.name,
      default: defaultZone,
      master: master,
      zone_endpoints: endpoints,
      access_key: zone.system_key.access_key,
      secret_key: zone.system_key.secret_key
    });
    return this.http.post(`${this.url}`, null, { params: params });
  }

  list(): Observable<object> {
    return this.http.get<object>(`${this.url}`);
  }

  get(zone: RgwZone): Observable<object> {
    return this.http.get(`${this.url}/${zone.name}`);
  }

  getAllZonesInfo(): Observable<object> {
    return this.http.get(`${this.url}/get_all_zones_info`);
  }

  delete(
    zoneName: string,
    deletePools: boolean,
    pools: Set<string>,
    zonegroupName: string
  ): Observable<any> {
    let params = new HttpParams();
    params = params.appendAll({
      zone_name: zoneName,
      delete_pools: deletePools,
      pools: Array.from(pools.values()),
      zonegroup_name: zonegroupName
    });
    return this.http.delete(`${this.url}/${zoneName}`, { params: params });
  }

  update(
    zone: RgwZone,
    zonegroup: RgwZonegroup,
    newZoneName: string,
    defaultZone?: boolean,
    master?: boolean,
    endpoints?: string,
    placementTarget?: string,
    dataPool?: string,
    indexPool?: string,
    dataExtraPool?: string,
    storageClass?: string,
    dataPoolClass?: string,
    compression?: string
  ) {
    let requestBody = {
      zone_name: zone.name,
      zonegroup_name: zonegroup.name,
      new_zone_name: newZoneName,
      default: defaultZone,
      master: master,
      zone_endpoints: endpoints,
      access_key: zone.system_key.access_key,
      secret_key: zone.system_key.secret_key,
      placement_target: placementTarget,
      data_pool: dataPool,
      index_pool: indexPool,
      data_extra_pool: dataExtraPool,
      storage_class: storageClass,
      data_pool_class: dataPoolClass,
      compression: compression
    };
    return this.http.put(`${this.url}/${zone.name}`, requestBody);
  }

  getZoneTree(
    zone: RgwZone,
    defaultZoneId: string,
    zones: RgwZone[],
    zonegroup?: RgwZonegroup,
    realm?: RgwRealm
  ) {
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
    const zoneNames = zones.map((zone: RgwZone) => {
      return zone['name'];
    });
    nodes['secondary_zone'] = !zoneNames.includes(zone.name) ? true : false;
    const zoneInfo = zones.filter((zoneInfo) => zoneInfo.name === zone.name);
    if (zoneInfo && zoneInfo.length > 0) {
      const access_key = zoneInfo[0].system_key['access_key'];
      const secret_key = zoneInfo[0].system_key['secret_key'];
      nodes['access_key'] = access_key ? access_key : '';
      nodes['secret_key'] = secret_key ? secret_key : '';
      nodes['user'] = access_key && access_key !== '' ? true : false;
    }
    if (nodes['access_key'] === '' || nodes['access_key'] === 'null') {
      nodes['show_warning'] = true;
      nodes['warning_message'] = 'Access/Secret keys not found';
    } else {
      nodes['show_warning'] = false;
    }
    if (nodes['endpoints'] && nodes['endpoints'].length === 0) {
      nodes['show_warning'] = true;
      nodes['warning_message'] = nodes['warning_message'] + '\n' + 'Endpoints not configured';
    }
    return {
      nodes: nodes,
      zoneIds: zoneIds
    };
  }

  getPoolNames() {
    return this.http.get(`${this.url}/get_pool_names`);
  }

  createSystemUser(userName: string, zone: string) {
    let requestBody = {
      userName: userName,
      zoneName: zone
    };
    return this.http.put(`${this.url}/create_system_user`, requestBody);
  }

  getUserList(zoneName: string) {
    let params = new HttpParams();
    params = params.appendAll({
      zoneName: zoneName
    });
    return this.http.get(`${this.url}/get_user_list`, { params: params });
  }
}
