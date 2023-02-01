import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { RgwRealm } from '~/app/ceph/rgw/models/rgw-multisite';
import { Icons } from '../enum/icons.enum';
import { RgwDaemonService } from './rgw-daemon.service';

@Injectable({
  providedIn: 'root'
})
export class RgwRealmService {
  private url = 'api/rgw/realm';

  constructor(private http: HttpClient, private rgwDaemonService: RgwDaemonService) {}

  create(realm: RgwRealm, defaultRealm: boolean) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        realm_name: realm.name,
        default: defaultRealm
      });
      return this.http.post(`${this.url}`, null, { params: params });
    });
  }

  list(): Observable<object> {
    return this.rgwDaemonService.request(() => {
      return this.http.get<object>(`${this.url}`);
    });
  }

  get(realm: RgwRealm): Observable<RgwRealm> {
    return this.rgwDaemonService.request(() => {
      return this.http.get(`${this.url}/${realm.name}`);
    });
  }

  getAllRealmsInfo(): Observable<object> {
    return this.rgwDaemonService.request(() => {
      return this.http.get(`${this.url}/get_all_realms_info`);
    });
  }

  getRealmTree(realm: RgwRealm, defaultRealmId: string) {
    let nodes = {};
    let realmIds = [];
    nodes['id'] = realm.id;
    realmIds.push(realm.id);
    nodes['name'] = realm.name + ' (realm)';
    nodes['info'] = realm;
    nodes['is_default'] = realm.id === defaultRealmId ? true : false;
    nodes['icon'] = Icons.reweight;
    return {
      nodes: nodes,
      realmIds: realmIds
    };
  }
}
