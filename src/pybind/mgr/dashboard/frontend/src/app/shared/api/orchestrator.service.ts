import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { Observable, of as observableOf } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { InventoryDevice } from '../../ceph/cluster/inventory/inventory-devices/inventory-device.model';
import { InventoryHost } from '../../ceph/cluster/inventory/inventory-host.model';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class OrchestratorService {
  private url = 'api/orchestrator';

  constructor(private http: HttpClient) {}

  status(): Observable<{ available: boolean; description: string }> {
    return this.http.get<{ available: boolean; description: string }>(`${this.url}/status`);
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
