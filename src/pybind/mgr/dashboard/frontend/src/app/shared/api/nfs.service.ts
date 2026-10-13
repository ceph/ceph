import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable, throwError } from 'rxjs';
import { NFSCluster } from '~/app/ceph/nfs/models/nfs.model';
import { BwTypeItem, NFSBwIopConfig, QOSTypeItem } from '~/app/ceph/nfs/models/nfs-cluster-config';
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
  qosType: QOSTypeItem[] = [
    {
      key: $localize`Shares only`,
      value: 'PerShare',
      help: $localize`Allows individual per share setting of export and client bandwidth`
    },
    {
      key: $localize`Clients only`,
      value: 'PerClient',
      help: $localize`Allows individual per client setting of export and client bandwidth`
    },
    {
      key: $localize`Shares and clients both`,
      value: 'PerShare_PerClient',
      help: $localize`Allows individual per share and per client setting of export and client bandwidth`
    }
  ];
  qosiopsType: QOSTypeItem[] = [
    {
      key: $localize`Each share`,
      value: 'PerShare',
      help: $localize`Allows individual per share setting of export and client IOPS`
    },
    {
      key: $localize`Each client`,
      value: 'PerClient',
      help: $localize`Allows individual per client setting of export and client IOPS`
    },
    {
      key: $localize`Each share and client`,
      value: 'PerShare_PerClient',
      help: $localize`Allows individual per share and per client setting of export and client IOPS`
    }
  ];
  bwType: BwTypeItem[] = [
    {
      value: 'Individual',
      help: $localize`Allows Individual IOPS limit for read and write operations`
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

  list(clusterId?: string) {
    return this.http.get(`${this.apiPath}/export`, {
      params: clusterId ? { cluster_id: clusterId } : {}
    });
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

  listClusters(info = true): Observable<NFSCluster[]> {
    return this.http.get<NFSCluster[]>(`${this.apiPath}/cluster`, {
      headers: { Accept: this.getVersionHeaderValue(1, 0) },
      params: { info: String(info) }
    });
  }

  getCluster(clusterId: string): Observable<NFSCluster> {
    return this.http.get<NFSCluster>(`${this.apiPath}/cluster/${encodeURIComponent(clusterId)}`, {
      headers: { Accept: this.getVersionHeaderValue(1, 0) }
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

  enableQosBandwidthForCLuster(obj: NFSBwIopConfig) {
    return this.http.patch(`${this.apiPath}/cluster/qos/bw`, obj, {
      headers: { Accept: this.getVersionHeaderValue(1, 0) },
      observe: 'response'
    });
  }

  getClusterBandwidthOpsConfig(clusterId: string) {
    return this.http.get(`${this.apiPath}/cluster/qos/${clusterId}`);
  }

  enableQosForExports(exportObj: NFSBwIopConfig) {
    return this.http.patch(`${this.apiPath}/export/qos`, exportObj, {
      headers: { Accept: this.getVersionHeaderValue(1, 0) },
      observe: 'response'
    });
  }

  enableQosOpsForExports(exportObj: NFSBwIopConfig) {
    return this.http.patch(`${this.apiPath}/export/qos/ops`, exportObj, {
      headers: { Accept: this.getVersionHeaderValue(1, 0) },
      observe: 'response'
    });
  }

  getexportBandwidthOpsConfig(clusterId: string, pseudoPath: string) {
    return this.http.get(
      `${this.apiPath}/export/qos/${clusterId}/${encodeURIComponent(pseudoPath)}`
    );
  }

  enableQosOpsForCLuster(obj: NFSBwIopConfig) {
    return this.http.patch(`${this.apiPath}/cluster/qos/ops`, obj, {
      headers: { Accept: this.getVersionHeaderValue(1, 0) },
      observe: 'response'
    });
  }
}
