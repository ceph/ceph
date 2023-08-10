import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { CephfsSubvolumeGroup } from '../models/cephfs-subvolume-group.model';

@Injectable({
  providedIn: 'root'
})
export class CephfsSubvolumeGroupService {
  baseURL = 'api/cephfs/subvolume/group';

  constructor(private http: HttpClient) {}

  get(volName: string): Observable<CephfsSubvolumeGroup[]> {
    return this.http.get<CephfsSubvolumeGroup[]>(`${this.baseURL}/${volName}`);
  }
}
