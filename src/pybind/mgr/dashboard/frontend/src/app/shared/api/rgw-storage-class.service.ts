import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { RequestModel } from '~/app/ceph/rgw/models/rgw-storage-class.model';

@Injectable({
  providedIn: 'root'
})
export class RgwStorageClassService {
  private url = 'api/rgw/zonegroup/storage-class';

  constructor(private http: HttpClient) {}

  removeStorageClass(placement_target: string, storage_class: string) {
    return this.http.delete(`${this.url}/${placement_target}/${storage_class}`, {
      observe: 'response'
    });
  }

  createStorageClass(requestModel: RequestModel) {
    return this.http.post(`${this.url}`, requestModel);
  }
}
