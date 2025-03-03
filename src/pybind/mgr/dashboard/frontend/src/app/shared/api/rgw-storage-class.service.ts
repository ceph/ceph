import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { RequestModel } from '~/app/ceph/rgw/models/rgw-storage-class.model';

@Injectable({
  providedIn: 'root'
})
export class RgwStorageClassService {
  private baseUrl = 'api/rgw/zonegroup';
  private url = `${this.baseUrl}/storage-class`;

  constructor(private http: HttpClient) {}

  removeStorageClass(placement_target: string, storage_class: string) {
    return this.http.delete(`${this.url}/${placement_target}/${storage_class}`, {
      observe: 'response'
    });
  }

  createStorageClass(requestModel: RequestModel) {
    return this.http.post(`${this.url}`, requestModel);
  }

  editStorageClass(requestModel: RequestModel) {
    return this.http.put(`${this.url}`, requestModel);
  }

  getPlacement_target(placement_id: string) {
    return this.http.get(`${this.baseUrl}/get_placement_target_by_placement_id/${placement_id}`);
  }
}
