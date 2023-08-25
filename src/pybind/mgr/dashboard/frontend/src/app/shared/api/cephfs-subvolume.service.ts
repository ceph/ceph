import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { CephfsSubvolume } from '../models/cephfs-subvolume.model';
import { Observable, of } from 'rxjs';
import { catchError, mapTo } from 'rxjs/operators';
import _ from 'lodash';

@Injectable({
  providedIn: 'root'
})
export class CephfsSubvolumeService {
  baseURL = 'api/cephfs/subvolume';

  constructor(private http: HttpClient) {}

  get(fsName: string): Observable<CephfsSubvolume[]> {
    return this.http.get<CephfsSubvolume[]>(`${this.baseURL}/${fsName}`);
  }

  create(
    fsName: string,
    subVolumeName: string,
    poolName: string,
    size: string,
    uid: number,
    gid: number,
    mode: string,
    namespace: boolean
  ) {
    return this.http.post(
      this.baseURL,
      {
        vol_name: fsName,
        subvol_name: subVolumeName,
        pool_layout: poolName,
        size: size,
        uid: uid,
        gid: gid,
        mode: mode,
        namespace_isolated: namespace
      },
      { observe: 'response' }
    );
  }

  info(fsName: string, subVolumeName: string) {
    return this.http.get(`${this.baseURL}/${fsName}/info`, {
      params: {
        subvol_name: subVolumeName
      }
    });
  }

  remove(fsName: string, subVolumeName: string) {
    return this.http.delete(`${this.baseURL}/${fsName}`, {
      params: {
        subvol_name: subVolumeName
      },
      observe: 'response'
    });
  }

  exists(subVolumeName: string, fsName: string) {
    return this.info(fsName, subVolumeName).pipe(
      mapTo(true),
      catchError((error: Event) => {
        if (_.isFunction(error.preventDefault)) {
          error.preventDefault();
        }
        return of(false);
      })
    );
  }

  update(fsName: string, subVolumeName: string, size: string) {
    return this.http.put(`${this.baseURL}/${fsName}`, {
      subvol_name: subVolumeName,
      size: size
    });
  }
}
