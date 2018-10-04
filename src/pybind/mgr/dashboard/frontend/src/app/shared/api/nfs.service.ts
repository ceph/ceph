import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';

import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class NfsService {
  apiPath = 'api/nfs-ganesha';
  uiApiPath = 'ui-api/nfs-ganesha';

  nfsAccessType = [
    {
      value: 'RW',
      help: this.i18n('Allows all operations')
    },
    {
      value: 'RO',
      help: this.i18n('Allows only operations that do not modify the server')
    },
    {
      value: 'MDONLY',
      help: this.i18n('Does not allow read or write operations, but allows any other operation')
    },
    {
      value: 'MDONLY_RO',
      help: this.i18n(
        'Does not allow read, write, or any operation that modifies file \
       attributes or directory content'
      )
    },
    {
      value: 'NONE',
      help: this.i18n('Allows no access at all')
    }
  ];

  nfsFsal = [
    {
      value: 'CEPH',
      descr: this.i18n('CephFS')
    },
    {
      value: 'RGW',
      descr: this.i18n('Object Gateway')
    }
  ];

  nfsSquash = ['no_root_squash', 'root_id_squash', 'root_squash', 'all_squash'];

  constructor(private http: HttpClient, private i18n: I18n) {}

  list() {
    return this.http.get(`${this.apiPath}/export`);
  }

  get(clusterId, exportId) {
    return this.http.get(`${this.apiPath}/export/${clusterId}/${exportId}`);
  }

  create(nfs) {
    return this.http.post(`${this.apiPath}/export`, nfs, { observe: 'response' });
  }

  update(clusterId, id, nfs) {
    return this.http.put(`${this.apiPath}/export/${clusterId}/${id}`, nfs, { observe: 'response' });
  }

  delete(clusterId, exportId) {
    return this.http.delete(`${this.apiPath}/export/${clusterId}/${exportId}`, {
      observe: 'response'
    });
  }

  lsDir(root_dir) {
    return this.http.get(`${this.uiApiPath}/lsdir?root_dir=${root_dir}`);
  }

  buckets(user_id) {
    return this.http.get(`${this.uiApiPath}/rgw/buckets?user_id=${user_id}`);
  }

  clients() {
    return this.http.get(`${this.uiApiPath}/cephx/clients`);
  }

  fsals() {
    return this.http.get(`${this.uiApiPath}/fsals`);
  }

  filesystems() {
    return this.http.get(`${this.uiApiPath}/cephfs/filesystems`);
  }

  daemon() {
    return this.http.get(`${this.apiPath}/daemon`);
  }

  start(host_name: string) {
    return this.http.put(`${this.apiPath}/service/${host_name}/start`, null, {
      observe: 'response'
    });
  }

  stop(host_name: string) {
    return this.http.put(`${this.apiPath}/service/${host_name}/stop`, null, {
      observe: 'response'
    });
  }
}
