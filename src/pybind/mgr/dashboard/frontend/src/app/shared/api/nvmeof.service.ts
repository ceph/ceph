import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import _ from 'lodash';
import { Observable, of as observableOf } from 'rxjs';
import { catchError, mapTo } from 'rxjs/operators';

export const MAX_NAMESPACE = 1024;

export interface ListenerRequest {
  host_name: string;
  traddr: string;
  trsvcid: number;
}

export interface NamespaceCreateRequest {
  rbd_image_name: string;
  rbd_pool: string;
  size: number;
}

export interface NamespaceEditRequest {
  rbd_image_size: number;
}

export interface InitiatorRequest {
  host_nqn: string;
}

const API_PATH = 'api/nvmeof';
const UI_API_PATH = 'ui-api/nvmeof';

@Injectable({
  providedIn: 'root'
})
export class NvmeofService {
  constructor(private http: HttpClient) {}

  // Gateway groups
  listGatewayGroups() {
    return this.http.get(`${API_PATH}/gateway/group`);
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
  getInitiators(subsystemNQN: string) {
    return this.http.get(`${API_PATH}/subsystem/${subsystemNQN}/host`);
  }

  addInitiators(subsystemNQN: string, request: InitiatorRequest) {
    return this.http.post(`${UI_API_PATH}/subsystem/${subsystemNQN}/host`, request, {
      observe: 'response'
    });
  }

  removeInitiators(subsystemNQN: string, request: InitiatorRequest) {
    return this.http.delete(`${UI_API_PATH}/subsystem/${subsystemNQN}/host/${request.host_nqn}`, {
      observe: 'response'
    });
  }

  // Listeners
  listListeners(subsystemNQN: string) {
    return this.http.get(`${API_PATH}/subsystem/${subsystemNQN}/listener`);
  }

  createListener(subsystemNQN: string, request: ListenerRequest) {
    return this.http.post(`${API_PATH}/subsystem/${subsystemNQN}/listener`, request, {
      observe: 'response'
    });
  }

  deleteListener(subsystemNQN: string, hostName: string, traddr: string, trsvcid: string) {
    return this.http.delete(
      `${API_PATH}/subsystem/${subsystemNQN}/listener/${hostName}/${traddr}`,
      {
        observe: 'response',
        params: {
          trsvcid
        }
      }
    );
  }

  // Namespaces
  listNamespaces(subsystemNQN: string) {
    return this.http.get(`${API_PATH}/subsystem/${subsystemNQN}/namespace`);
  }

  getNamespace(subsystemNQN: string, nsid: string) {
    return this.http.get(`${API_PATH}/subsystem/${subsystemNQN}/namespace/${nsid}`);
  }

  createNamespace(subsystemNQN: string, request: NamespaceCreateRequest) {
    return this.http.post(`${API_PATH}/subsystem/${subsystemNQN}/namespace`, request, {
      observe: 'response'
    });
  }

  updateNamespace(subsystemNQN: string, nsid: string, request: NamespaceEditRequest) {
    return this.http.patch(`${API_PATH}/subsystem/${subsystemNQN}/namespace/${nsid}`, request, {
      observe: 'response'
    });
  }

  deleteNamespace(subsystemNQN: string, nsid: string) {
    return this.http.delete(`${API_PATH}/subsystem/${subsystemNQN}/namespace/${nsid}`, {
      observe: 'response'
    });
  }
}
