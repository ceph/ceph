import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable, throwError } from 'rxjs';

import { NfsFSAbstractionLayer, SUPPORTED_FSAL } from '~/app/ceph/nfs/models/nfs.fsal';
import { ApiClient } from '~/app/shared/api/api-client';

export interface Directory {
  paths: string[];
}

@Injectable({
  providedIn: 'root'
})
export class NfsService extends ApiClient {
  apiPath = 'api/nfs-ganesha';
  uiApiPath = 'ui-api/nfs-ganesha';

  nfsAccessType = [
    {
      value: 'RW',
      help: $localize`Allows all operations`
    },
    {
      value: 'RO',
      help: $localize`Allows only operations that do not modify the server`
    },
    {
      value: 'NONE',
      help: $localize`Allows no access at all`
    }
  ];

  nfsFsal: NfsFSAbstractionLayer[] = [
    {
      value: SUPPORTED_FSAL.CEPH,
      descr: $localize`CephFS`,
      disabled: false
    },
    {
      value: SUPPORTED_FSAL.RGW,
      descr: $localize`Object Gateway`,
      disabled: false
    }
  ];

  nfsSquash = {
    no_root_squash: ['no_root_squash', 'noidsquash', 'none'],
    root_id_squash: ['root_id_squash', 'rootidsquash', 'rootid'],
    root_squash: ['root_squash', 'rootsquash', 'root'],
    all_squash: ['all_squash', 'allsquash', 'all', 'allanonymous', 'all_anonymous']
  };

  constructor(private http: HttpClient) {
    super();
  }

  list() {
    return this.http.get(`${this.apiPath}/export`);
  }

  get(clusterId: string, exportId: string) {
    return this.http.get(`${this.apiPath}/export/${clusterId}/${exportId}`);
  }

  create(nfs: any) {
    return this.http.post(`${this.apiPath}/export`, nfs, {
      headers: { Accept: this.getVersionHeaderValue(2, 0) },
      observe: 'response'
    });
  }

  update(clusterId: string, id: number, nfs: any) {
    return this.http.put(`${this.apiPath}/export/${clusterId}/${id}`, nfs, {
      headers: { Accept: this.getVersionHeaderValue(2, 0) },
      observe: 'response'
    });
  }

  delete(clusterId: string, exportId: string) {
    return this.http.delete(`${this.apiPath}/export/${clusterId}/${exportId}`, {
      headers: { Accept: this.getVersionHeaderValue(2, 0) },
      observe: 'response'
    });
  }

  listClusters() {
    return this.http.get(`${this.apiPath}/cluster`, {
      headers: { Accept: this.getVersionHeaderValue(0, 1) }
    });
  }

  lsDir(fs_name: string, root_dir: string): Observable<Directory> {
    if (!fs_name) {
      return throwError($localize`Please specify a filesystem volume.`);
    }
    return this.http.get<Directory>(`${this.uiApiPath}/lsdir/${fs_name}?root_dir=${root_dir}`);
  }

  fsals() {
    return this.http.get(`${this.uiApiPath}/fsals`);
  }

  filesystems() {
    return this.http.get(`${this.uiApiPath}/cephfs/filesystems`);
  }
}
