import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { CephfsSubvolume } from '../models/cephfs-subvolume.model';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class CephfsSubvolumeService {
  baseURL = 'api/cephfs/subvolume';

  constructor(private http: HttpClient) {}

  get(fsName: string): Observable<CephfsSubvolume[]> {
    return this.http.get<CephfsSubvolume[]>(`${this.baseURL}/${fsName}`);
  }
}
