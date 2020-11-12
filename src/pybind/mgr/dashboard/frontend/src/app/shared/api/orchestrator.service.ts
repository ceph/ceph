import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { Observable, of as observableOf } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { InventoryDevice } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-device.model';
import { InventoryHost } from '~/app/ceph/cluster/inventory/inventory-host.model';
import { OrchestratorFeature } from '../models/orchestrator.enum';
import { OrchestratorStatus } from '../models/orchestrator.interface';

@Injectable({
  providedIn: 'root'
})
export class OrchestratorService {
  private url = 'api/orchestrator';

  disableMessages = {
    noOrchestrator: $localize`The feature is disabled because Orchestrator is not available.`,
    missingFeature: $localize`The Orchestrator backend doesn't support this feature.`
  };

  constructor(private http: HttpClient) {}

  status(): Observable<OrchestratorStatus> {
    return this.http.get<OrchestratorStatus>(`${this.url}/status`);
  }

  hasFeature(status: OrchestratorStatus, features: OrchestratorFeature[]): boolean {
    return _.every(features, (feature) => _.get(status.features, `${feature}.available`));
  }

  getTableActionDisableDesc(
    status: OrchestratorStatus,
    features: OrchestratorFeature[]
  ): boolean | string {
    if (!status) {
      return false;
    }
    if (!status.available) {
      return this.disableMessages.noOrchestrator;
    }
    if (!this.hasFeature(status, features)) {
      return this.disableMessages.missingFeature;
    }
    return false;
  }

  identifyDevice(hostname: string, device: string, duration: number) {
    return this.http.post(`${this.url}/identify_device`, {
      hostname,
      device,
      duration
    });
  }

  inventoryList(hostname?: string, refresh?: boolean): Observable<InventoryHost[]> {
    let params = new HttpParams();
    if (hostname) {
      params = params.append('hostname', hostname);
    }
    if (refresh) {
      params = params.append('refresh', _.toString(refresh));
    }
    return this.http.get<InventoryHost[]>(`${this.url}/inventory`, { params: params });
  }

  inventoryDeviceList(hostname?: string, refresh?: boolean): Observable<InventoryDevice[]> {
    return this.inventoryList(hostname, refresh).pipe(
      mergeMap((hosts: InventoryHost[]) => {
        const devices = _.flatMap(hosts, (host) => {
          return host.devices.map((device) => {
            device.hostname = host.name;
            device.uid = device.device_id ? device.device_id : `${device.hostname}-${device.path}`;
            return device;
          });
        });
        return observableOf(devices);
      })
    );
  }
}
