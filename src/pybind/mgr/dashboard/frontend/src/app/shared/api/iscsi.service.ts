import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { cdEncode } from '../decorators/cd-encode';

@cdEncode
@Injectable({
  providedIn: 'root'
})
export class IscsiService {
  constructor(private http: HttpClient) {}

  listTargets() {
    return this.http.get(`api/iscsi/target`);
  }

  getTarget(target_iqn: string) {
    return this.http.get(`api/iscsi/target/${target_iqn}`);
  }

  updateTarget(target_iqn: string, target: any) {
    return this.http.put(`api/iscsi/target/${target_iqn}`, target, { observe: 'response' });
  }

  status() {
    return this.http.get(`ui-api/iscsi/status`);
  }

  settings() {
    return this.http.get(`ui-api/iscsi/settings`);
  }

  version() {
    return this.http.get(`ui-api/iscsi/version`);
  }

  portals() {
    return this.http.get(`ui-api/iscsi/portals`);
  }

  createTarget(target: any) {
    return this.http.post(`api/iscsi/target`, target, { observe: 'response' });
  }

  deleteTarget(target_iqn: string) {
    return this.http.delete(`api/iscsi/target/${target_iqn}`, { observe: 'response' });
  }

  getDiscovery() {
    return this.http.get(`api/iscsi/discoveryauth`);
  }

  updateDiscovery(auth: any) {
    return this.http.put(`api/iscsi/discoveryauth`, auth);
  }

  overview() {
    return this.http.get(`ui-api/iscsi/overview`);
  }
}
