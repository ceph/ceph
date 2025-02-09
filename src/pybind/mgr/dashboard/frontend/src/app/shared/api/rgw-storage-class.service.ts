import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
@Injectable({
  providedIn: 'root'
})
export class RgwStorageClassService {
  private url = 'api/rgw/zonegroup';

  constructor(private http: HttpClient) {}

  removeStorageClass(placement_target: string, storage_class: string) {
    return this.http.delete(`${this.url}/storage-class/${placement_target}/${storage_class}`, {
      observe: 'response'
    });
  }
}
