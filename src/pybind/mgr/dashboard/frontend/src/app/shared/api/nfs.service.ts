import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class NfsService {
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
      value: 'MDONLY',
      help: $localize`Does not allow read or write operations, but allows any other operation`
    },
    {
      value: 'MDONLY_RO',
      help: $localize`Does not allow read, write, or any operation that modifies file attributes or directory content`
    },
    {
      value: 'NONE',
      help: $localize`Allows no access at all`
    }
  ];

  nfsFsal = [
    {
      value: 'CEPH',
      descr: $localize`CephFS`
    },
    {
      value: 'RGW',
      descr: $localize`Object Gateway`
    }
  ];

  nfsSquash = ['no_root_squash', 'root_id_squash', 'root_squash', 'all_squash'];

  constructor(private http: HttpClient) {}

  list() {
    return this.http.get(`${this.apiPath}/export`);
  }

  get(clusterId: string, exportId: string) {
    return this.http.get(`${this.apiPath}/export/${clusterId}/${exportId}`);
  }

  create(nfs: any) {
    return this.http.post(`${this.apiPath}/export`, nfs, { observe: 'response' });
  }

  update(clusterId: string, id: string, nfs: any) {
    return this.http.put(`${this.apiPath}/export/${clusterId}/${id}`, nfs, { observe: 'response' });
  }

  delete(clusterId: string, exportId: string) {
    return this.http.delete(`${this.apiPath}/export/${clusterId}/${exportId}`, {
      observe: 'response'
    });
  }

  lsDir(root_dir: string) {
    return this.http.get(`${this.uiApiPath}/lsdir?root_dir=${root_dir}`);
  }

  buckets(user_id: string) {
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
