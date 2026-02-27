import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import _ from 'lodash';
import { Observable, forkJoin, of as observableOf } from 'rxjs';
import { catchError, map, mapTo, mergeMap } from 'rxjs/operators';
import { CephServiceSpec } from '../models/service.interface';
import { ListenerItem } from '../models/nvmeof';
import { HostService } from './host.service';
import { OrchestratorService } from './orchestrator.service';
import { HostStatus } from '../enum/host-status.enum';
import { Host } from '../models/host.interface';
import { OrchestratorStatus } from '../models/orchestrator.interface';

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
  rbd_image_name?: string;
  rbd_pool: string;
  rbd_image_size?: number;
  no_auto_visible?: boolean;
  block_size?: number;
  create_image: boolean;
};

export type NamespaceUpdateRequest = NvmeofRequest & {
  rbd_image_size: number;
};

export type InitiatorRequest = NvmeofRequest & {
  host_nqn: string;
  dhchap_key?: string;
};

export type NamespaceInitiatorRequest = InitiatorRequest & {
  subsystem_nqn: string;
};

const API_PATH = 'api/nvmeof';
const UI_API_PATH = 'ui-api/nvmeof';

@Injectable({
  providedIn: 'root'
})
export class NvmeofService {
  constructor(
    private http: HttpClient,
    private hostService: HostService,
    private orchService: OrchestratorService
  ) {}

  getAvailableHosts(params: any = {}): Observable<Host[]> {
    return forkJoin({
      groups: this.listGatewayGroups(),
      hosts: this.orchService.status().pipe(
        mergeMap((orchStatus: OrchestratorStatus) => {
          const factsAvailable = this.hostService.checkHostsFactsAvailable(orchStatus);
          return this.hostService.list(params, factsAvailable.toString()) as Observable<Host[]>;
        }),
        map((hosts: Host[]) => {
          return (hosts || []).map((host: Host) => ({
            ...host,
            status: host.status || HostStatus.AVAILABLE
          }));
        })
      )
    }).pipe(
      map(({ groups, hosts }) => {
        const usedHosts = new Set<string>();
        (groups?.[0] ?? []).forEach((group: CephServiceSpec) => {
          group.placement?.hosts?.forEach((hostname: string) => usedHosts.add(hostname));
        });
        return (hosts || []).filter((host: Host) => {
          const isAvailable =
            host.status === HostStatus.AVAILABLE || host.status === HostStatus.RUNNING;
          return !usedHosts.has(host.hostname) && isAvailable;
        });
      })
    );
  }

  fetchHostsAndGroups(): Observable<{ groups: CephServiceSpec[][]; hosts: Host[] }> {
    return forkJoin({
      groups: this.listGatewayGroups(),
      hosts: this.hostService.getAllHosts().pipe(
        map((hosts: Host[]) => {
          return (hosts || []).map((host: Host) => ({
            ...host,
            status: host.status || HostStatus.AVAILABLE
          }));
        })
      )
    });
  }

  getHostsForGroup(groupName: string): Observable<Host[]> {
    return forkJoin({
      gwGroups: this.listGatewayGroups(),
      allHosts: this.hostService.getAllHosts()
    }).pipe(
      map(({ gwGroups, allHosts }) => {
        const group = gwGroups?.[0]?.find(
          (gwGroup: CephServiceSpec) => gwGroup?.spec?.group === groupName
        );
        const placement = group?.placement || { hosts: [], label: [] };
        const { hosts, label } = placement;

        if (hosts?.length) {
          return allHosts.filter((host: Host) => hosts.includes(host.hostname));
        } else if (label?.length) {
          if (typeof label === 'string') {
            return allHosts.filter((host: Host) => host?.labels?.includes(label));
          }
          return allHosts.filter(
            (host: Host) =>
              host?.labels?.length === label?.length &&
              _.isEqual([...host.labels].sort(), [...label].sort())
          );
        }
        return [];
      })
    );
  }

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
    dhchap_key: string;
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

  addNamespaceInitiators(nsid: number | string, request: NamespaceInitiatorRequest) {
    return this.http.post(
      `${UI_API_PATH}/subsystem/${request.subsystem_nqn}/namespace/${nsid}/host`,
      request,
      {
        observe: 'response'
      }
    );
  }

  updateHostKey(subsystemNQN: string, request: InitiatorRequest) {
    return this.http.put(
      `${API_PATH}/subsystem/${subsystemNQN}/host/${request.host_nqn}/change_key`,
      request,
      {
        observe: 'response'
      }
    );
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

  createListeners(subsystemNQN: string, gwGroup: string, listeners: ListenerItem[]) {
    const listenerCalls = listeners.map((listener: ListenerItem) =>
      this.createListener(subsystemNQN, {
        gw_group: gwGroup,
        host_name: listener.content,
        traddr: listener.addr,
        trsvcid: 4420
      })
    );
    return forkJoin(listenerCalls);
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
  listNamespaces(group: string, subsystemNQN: string = '*') {
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

  // Check if gateway group exists
  exists(groupName: string): Observable<boolean> {
    return this.listGatewayGroups().pipe(
      map((groups: CephServiceSpec[][]) => {
        const groupsList = groups?.[0] ?? [];
        return groupsList.some((group: CephServiceSpec) => group?.spec?.group === groupName);
      }),
      catchError((error: any) => {
        if (_.isFunction(error?.preventDefault)) {
          error.preventDefault();
        }
        return observableOf(false);
      })
    );
  }
}
