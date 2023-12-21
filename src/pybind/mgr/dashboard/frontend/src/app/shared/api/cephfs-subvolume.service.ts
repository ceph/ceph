import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { CephfsSubvolume, SubvolumeSnapshot } from '../models/cephfs-subvolume.model';
import { Observable, of } from 'rxjs';
import { catchError, mapTo } from 'rxjs/operators';
import _ from 'lodash';

@Injectable({
  providedIn: 'root'
})
export class CephfsSubvolumeService {
  baseURL = 'api/cephfs/subvolume';

  constructor(private http: HttpClient) {}

  get(fsName: string, subVolumeGroupName: string = '', info = true): Observable<CephfsSubvolume[]> {
    return this.http.get<CephfsSubvolume[]>(`${this.baseURL}/${fsName}`, {
      params: {
        group_name: subVolumeGroupName,
        info: info
      }
    });
  }

  create(
    fsName: string,
    subVolumeName: string,
    subVolumeGroupName: string,
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
        group_name: subVolumeGroupName,
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

  info(fsName: string, subVolumeName: string, subVolumeGroupName: string = '') {
    return this.http.get(`${this.baseURL}/${fsName}/info`, {
      params: {
        subvol_name: subVolumeName,
        group_name: subVolumeGroupName
      }
    });
  }

  remove(
    fsName: string,
    subVolumeName: string,
    subVolumeGroupName: string = '',
    retainSnapshots: boolean = false
  ) {
    return this.http.delete(`${this.baseURL}/${fsName}`, {
      params: {
        subvol_name: subVolumeName,
        group_name: subVolumeGroupName,
        retain_snapshots: retainSnapshots
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

  existsInFs(fsName: string, groupName = ''): Observable<boolean> {
    return this.http.get<boolean>(`${this.baseURL}/${fsName}/exists`, {
      params: {
        group_name: groupName
      }
    });
  }

  update(fsName: string, subVolumeName: string, size: string, subVolumeGroupName: string = '') {
    return this.http.put(`${this.baseURL}/${fsName}`, {
      subvol_name: subVolumeName,
      size: size,
      group_name: subVolumeGroupName
    });
  }

  getSnapshots(
    fsName: string,
    subVolumeName: string,
    groupName = ''
  ): Observable<SubvolumeSnapshot[]> {
    return this.http.get<SubvolumeSnapshot[]>(
      `${this.baseURL}/snapshot/${fsName}/${subVolumeName}`,
      {
        params: {
          group_name: groupName
        }
      }
    );
  }
}
