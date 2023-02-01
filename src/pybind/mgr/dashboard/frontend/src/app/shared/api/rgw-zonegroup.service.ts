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

  getZonegroupTree(zonegroup: RgwZonegroup, defaultZonegroupId: string, realm?: RgwRealm) {
    let nodes = {};
    nodes['id'] = zonegroup.id;
    nodes['name'] = zonegroup.name + ' (zonegroup)';
    nodes['info'] = zonegroup;
    nodes['icon'] = Icons.cubes;
    nodes['is_master'] = zonegroup.is_master;
    nodes['parent'] = realm ? realm.name : '';
    nodes['is_default'] = zonegroup.id === defaultZonegroupId ? true : false;
    return nodes;
  }
}
