import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import _ from 'lodash';
import { Observable, of as observableOf } from 'rxjs';
import { catchError, mapTo } from 'rxjs/operators';
import { CephServiceSpec } from '../models/service.interface';

export const DEFAULT_MAX_NAMESPACE_PER_SUBSYSTEM = 512;

export type GatewayGroup = CephServiceSpec;

export type GroupsComboboxItem = {
  content: string;
  serviceName?: string;
  selected?: boolean;
};

type NvmeofRequest = {
  gw_group: string;
};

export type ListenerRequest = NvmeofRequest & {
  host_name: string;
  traddr: string;
  trsvcid: number;
};

export type NamespaceCreateRequest = NvmeofRequest & {
  rbd_image_name: string;
  rbd_pool: string;
  rbd_image_size?: number;
  create_image: boolean;
};

export type NamespaceUpdateRequest = NvmeofRequest & {
  rbd_image_size: number;
};

export type InitiatorRequest = NvmeofRequest & {
  host_nqn: string;
};

const API_PATH = 'api/nvmeof';
const UI_API_PATH = 'ui-api/nvmeof';

@Injectable({
  providedIn: 'root'
})
export class NvmeofService {
  constructor(private http: HttpClient) {}

  // formats the gateway groups to be consumed for combobox item
  formatGwGroupsList(
    data: CephServiceSpec[][],
    isGatewayList: boolean = false
  ): GroupsComboboxItem[] {
    return data[0].reduce((gwGrpList: GroupsComboboxItem[], group: CephServiceSpec) => {
      if (isGatewayList && group?.spec?.group && group?.service_name) {
        gwGrpList.push({
          content: group.spec.group,
          serviceName: group.service_name
        });
      } else {
        if (group?.spec?.group) {
          gwGrpList.push({
            content: group.spec.group
          });
        }
      }
      return gwGrpList;
    }, []);
  }

  // Gateway groups
  listGatewayGroups() {
    return this.http.get<CephServiceSpec[][]>(`${API_PATH}/gateway/group`);
  }

  // Gateways
  listGateways() {
    return this.http.get(`${API_PATH}/gateway`);
  }

  // Subsystems
  listSubsystems(group: string) {
    return this.http.get(`${API_PATH}/subsystem?gw_group=${group}`);
  }

  getSubsystem(subsystemNQN: string, group: string) {
    return this.http.get(`${API_PATH}/subsystem/${subsystemNQN}?gw_group=${group}`);
  }

  createSubsystem(request: {
    nqn: string;
    enable_ha: boolean;
    gw_group: string;
    max_namespaces?: number;
  }) {
    return this.http.post(`${API_PATH}/subsystem`, request, { observe: 'response' });
  }

  deleteSubsystem(subsystemNQN: string, group: string) {
    return this.http.delete(`${API_PATH}/subsystem/${subsystemNQN}?gw_group=${group}`, {
      observe: 'response'
    });
  }

  isSubsystemPresent(subsystemNqn: string, group: string): Observable<boolean> {
    return this.getSubsystem(subsystemNqn, group).pipe(
      mapTo(true),
      catchError((e) => {
        e?.preventDefault();
        return observableOf(false);
      })
    );
  }

  // Initiators
  getInitiators(subsystemNQN: string, group: string) {
    return this.http.get(`${API_PATH}/subsystem/${subsystemNQN}/host?gw_group=${group}`);
  }

  addInitiators(subsystemNQN: string, request: InitiatorRequest) {
    return this.http.post(`${UI_API_PATH}/subsystem/${subsystemNQN}/host`, request, {
      observe: 'response'
    });
  }

  removeInitiators(subsystemNQN: string, request: InitiatorRequest) {
    return this.http.delete(
      `${UI_API_PATH}/subsystem/${subsystemNQN}/host/${request.host_nqn}/${request.gw_group}`,
      {
        observe: 'response'
      }
    );
  }

  // Listeners
  listListeners(subsystemNQN: string, group: string) {
    return this.http.get(`${API_PATH}/subsystem/${subsystemNQN}/listener?gw_group=${group}`);
  }

  createListener(subsystemNQN: string, request: ListenerRequest) {
    return this.http.post(`${API_PATH}/subsystem/${subsystemNQN}/listener`, request, {
      observe: 'response'
    });
  }

  deleteListener(
    subsystemNQN: string,
    group: string,
    hostName: string,
    traddr: string,
    trsvcid: string
  ) {
    return this.http.delete(
      `${API_PATH}/subsystem/${subsystemNQN}/listener/${hostName}/${traddr}`,
      {
        observe: 'response',
        params: {
          gw_group: group,
          trsvcid,
          force: 'true'
        }
      }
    );
  }

  // Namespaces
  listNamespaces(subsystemNQN: string, group: string) {
    return this.http.get(`${API_PATH}/subsystem/${subsystemNQN}/namespace?gw_group=${group}`);
  }

  getNamespace(subsystemNQN: string, nsid: string, group: string) {
    return this.http.get(
      `${API_PATH}/subsystem/${subsystemNQN}/namespace/${nsid}?gw_group=${group}`
    );
  }

  createNamespace(subsystemNQN: string, request: NamespaceCreateRequest) {
    return this.http.post(`${API_PATH}/subsystem/${subsystemNQN}/namespace`, request, {
      observe: 'response'
    });
  }

  updateNamespace(subsystemNQN: string, nsid: string, request: NamespaceUpdateRequest) {
    return this.http.patch(`${API_PATH}/subsystem/${subsystemNQN}/namespace/${nsid}`, request, {
      observe: 'response'
    });
  }

  deleteNamespace(subsystemNQN: string, nsid: string, group: string) {
    return this.http.delete(
      `${API_PATH}/subsystem/${subsystemNQN}/namespace/${nsid}?gw_group=${group}`,
      {
        observe: 'response'
      }
    );
  }

  getNamespaceList() {
    return observableOf([
      {
        bdev_name: 'bdev_ec4ee465-078a-4984-9c41-f546b0f60ff9',
        rbd_image_name: 'nvme_rbd_Test2_3sf8ajg5xrv',
        rbd_pool_name: 'rbd',
        load_balancing_group: 1,
        rbd_image_size: '1073741824',
        block_size: 512,
        rw_ios_per_second: '0',
        rw_mbytes_per_second: '0',
        r_mbytes_per_second: '0',
        w_mbytes_per_second: '0',
        auto_visible: true,
        hosts: [],
        nsid: 1,
        uuid: 'ec4ee465-078a-4984-9c41-f546b0f60ff9',
        ns_subsystem_nqn: 'nqn.2001-07.com.ceph:1768841282397.Test2',
        trash_image: false,
        disable_auto_resize: false,
        read_only: false
      },
      {
        bdev_name: 'bdev_90344a15-c16a-4266-9f65-f5978ef7fcf3',
        rbd_image_name: 'nvme_rbd_Test2_f8x9mv72yen',
        rbd_pool_name: 'rbd',
        load_balancing_group: 1,
        rbd_image_size: '1073741824',
        block_size: 512,
        rw_ios_per_second: '0',
        rw_mbytes_per_second: '0',
        r_mbytes_per_second: '0',
        w_mbytes_per_second: '0',
        auto_visible: true,
        hosts: [],
        nsid: 2,
        uuid: '90344a15-c16a-4266-9f65-f5978ef7fcf3',
        ns_subsystem_nqn: 'nqn.2001-07.com.ceph:1768841282397.Test2',
        trash_image: false,
        disable_auto_resize: false,
        read_only: false
      },
      {
        bdev_name: 'bdev_d2526f3a-9917-41d0-9878-1a95fcd8b7d8',
        rbd_image_name: 'nvme_rbd_Test2_e7mtstemdqm',
        rbd_pool_name: 'rbd',
        load_balancing_group: 1,
        rbd_image_size: '1073741824',
        block_size: 512,
        rw_ios_per_second: '0',
        rw_mbytes_per_second: '0',
        r_mbytes_per_second: '0',
        w_mbytes_per_second: '0',
        auto_visible: true,
        hosts: [],
        nsid: 3,
        uuid: 'd2526f3a-9917-41d0-9878-1a95fcd8b7d8',
        ns_subsystem_nqn: 'nqn.2001-07.com.ceph:1768841282397.Test2',
        trash_image: false,
        disable_auto_resize: false,
        read_only: false
      },
      {
        bdev_name: 'bdev_c6e40690-6cad-488c-8c2b-782bf77f305c',
        rbd_image_name: 'nvme_rbd_Test2_n8xpw7m2yrr',
        rbd_pool_name: 'rbd',
        load_balancing_group: 1,
        rbd_image_size: '1073741824',
        block_size: 512,
        rw_ios_per_second: '0',
        rw_mbytes_per_second: '0',
        r_mbytes_per_second: '0',
        w_mbytes_per_second: '0',
        auto_visible: true,
        hosts: [],
        nsid: 4,
        uuid: 'c6e40690-6cad-488c-8c2b-782bf77f305c',
        ns_subsystem_nqn: 'nqn.2001-07.com.ceph:1768841282397.Test2',
        trash_image: false,
        disable_auto_resize: false,
        read_only: false
      },
      {
        bdev_name: 'bdev_ec8c62fd-6f58-4921-9755-74cebd04432f',
        rbd_image_name: 'nvme_rbd_Test2_w5wgcmriii',
        rbd_pool_name: 'rbd',
        load_balancing_group: 1,
        rbd_image_size: '1073741824',
        block_size: 512,
        rw_ios_per_second: '0',
        rw_mbytes_per_second: '0',
        r_mbytes_per_second: '0',
        w_mbytes_per_second: '0',
        auto_visible: true,
        hosts: [],
        nsid: 5,
        uuid: 'ec8c62fd-6f58-4921-9755-74cebd04432f',
        ns_subsystem_nqn: 'nqn.2001-07.com.ceph:1768841282397.Test2',
        trash_image: false,
        disable_auto_resize: false,
        read_only: false
      }
    ]);
  }
}
