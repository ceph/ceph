import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { CephfsSubvolumeGroup } from '../models/cephfs-subvolumegroup.model';
import _ from 'lodash';
import { mapTo, catchError } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class CephfsSubvolumeGroupService {
  baseURL = 'api/cephfs/subvolume/group';

  constructor(private http: HttpClient) {}

  get(volName: string): Observable<CephfsSubvolumeGroup[]> {
    return this.http.get<CephfsSubvolumeGroup[]>(`${this.baseURL}/${volName}`);
  }

  create(
    volName: string,
    groupName: string,
    poolName: string,
    size: string,
    uid: number,
    gid: number,
    mode: string
  ) {
    return this.http.post(
      this.baseURL,
      {
        vol_name: volName,
        group_name: groupName,
        pool_layout: poolName,
        size: size,
        uid: uid,
        gid: gid,
        mode: mode
      },
      { observe: 'response' }
    );
  }

  info(volName: string, groupName: string) {
    return this.http.get(`${this.baseURL}/${volName}/info`, {
      params: {
        group_name: groupName
      }
    });
  }

  exists(groupName: string, volName: string) {
    return this.info(volName, groupName).pipe(
      mapTo(true),
      catchError((error: Event) => {
        if (_.isFunction(error.preventDefault)) {
          error.preventDefault();
        }
        return of(false);
      })
    );
  }

  update(volName: string, groupName: string, size: string) {
    return this.http.put(`${this.baseURL}/${volName}`, {
      group_name: groupName,
      size: size
    });
  }

  remove(volName: string, groupName: string) {
    return this.http.delete(`${this.baseURL}/${volName}`, {
      params: {
        group_name: groupName
      },
      observe: 'response'
    });
  }
}
