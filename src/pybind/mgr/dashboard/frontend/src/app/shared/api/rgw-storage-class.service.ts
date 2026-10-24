import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { RequestModel, ZoneRequest } from '~/app/ceph/rgw/models/rgw-storage-class.model';

@Injectable({
  providedIn: 'root'
})
export class RgwStorageClassService {
  private baseUrl = 'api/rgw/zonegroup';
  private zoneUrl = 'api/rgw/zone';
  private url = `${this.baseUrl}/storage-class`;

  constructor(private http: HttpClient) {}

  removeStorageClass(
    placement_target: string,
    storage_class: string,
    zone_name: string = '',
    zonegroup_name: string = ''
  ) {
    let params: any = {};
    if (zone_name) {
      params.zone_name = zone_name;
    }
    if (zonegroup_name) {
      params.zonegroup_name = zonegroup_name;
    }
    return this.http.delete(`${this.url}/${placement_target}/${storage_class}`, {
      params,
      observe: 'response'
    });
  }

  createStorageClass(requestModel: RequestModel) {
    return this.http.post(`${this.url}`, requestModel);
  }

  editStorageClass(requestModel: RequestModel) {
    return this.http.put(`${this.url}`, requestModel);
  }

  getPlacement_target(placement_id: string, zonegroup_name: string = '') {
    const params: any = zonegroup_name ? { zonegroup_name } : {};
    return this.http.get(`${this.baseUrl}/get_placement_target_by_placement_id/${placement_id}`, {
      params
    });
  }

  createStorageClassZone(zoneRequest: ZoneRequest) {
    return this.http.post(`${this.zoneUrl}/storage-class`, zoneRequest);
  }

  editStorageClassZone(zoneRequest: ZoneRequest) {
    return this.http.put(`${this.zoneUrl}/storage-class`, zoneRequest);
  }
}
