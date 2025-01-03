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

  constructor(private http: HttpClient, public rgwDaemonService: RgwDaemonService) {}

  create(realm: RgwRealm, defaultRealm: boolean) {
    let requestBody = {
      realm_name: realm.name,
      default: defaultRealm
    };
    return this.http.post(`${this.url}`, requestBody);
  }

  update(realm: RgwRealm, defaultRealm: boolean, newRealmName: string) {
    let requestBody = {
      realm_name: realm.name,
      default: defaultRealm,
      new_realm_name: newRealmName
    };
    return this.http.put(`${this.url}/${realm.name}`, requestBody);
  }

  list(): Observable<object> {
    return this.http.get<object>(`${this.url}`);
  }

  get(realm: RgwRealm): Observable<object> {
    return this.http.get(`${this.url}/${realm.name}`);
  }

  getAllRealmsInfo(): Observable<object> {
    return this.http.get(`${this.url}/get_all_realms_info`);
  }

  delete(realmName: string): Observable<any> {
    let params = new HttpParams();
    params = params.appendAll({
      realm_name: realmName
    });
    return this.http.delete(`${this.url}/${realmName}`, { params: params });
  }

  getRealmTree(realm: RgwRealm, defaultRealmId: string) {
    let nodes = {};
    let realmIds = [];
    nodes['id'] = realm.id;
    realmIds.push(realm.id);
    nodes['name'] = realm.name;
    nodes['info'] = realm;
    nodes['is_default'] = realm.id === defaultRealmId ? true : false;
    nodes['icon'] = Icons.reweight;
    nodes['type'] = 'realm';
    return {
      nodes: nodes,
      realmIds: realmIds
    };
  }

  importRealmToken(realm_token: string, zone_name: string, port: number, placementSpec: object) {
    let requestBody = {
      realm_token: realm_token,
      zone_name: zone_name,
      port: port,
      placement_spec: placementSpec
    };
    return this.http.post(`${this.url}/import_realm_token`, requestBody);
  }

  getRealmTokens() {
    return this.rgwDaemonService.request(() => {
      return this.http.get(`${this.url}/get_realm_tokens`);
    });
  }
}
