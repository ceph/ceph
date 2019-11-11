import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { Observable, of as observableOf } from 'rxjs';
import { mergeMap } from 'rxjs/operators';

import { InventoryDevice } from '../../ceph/cluster/inventory/inventory-devices/inventory-device.model';
import { InventoryNode } from '../../ceph/cluster/inventory/inventory-node.model';
import { ApiModule } from './api.module';

@Injectable({
  providedIn: ApiModule
})
export class OrchestratorService {
  private url = 'api/orchestrator';

  constructor(private http: HttpClient) {}

  status() {
    return this.http.get(`${this.url}/status`);
  }

  identifyDevice(hostname: string, device: string, duration: number) {
    return this.http.post(`${this.url}/identify_device`, {
      hostname,
      device,
      duration
    });
  }

  inventoryList(hostname?: string): Observable<InventoryNode[]> {
    const options = hostname ? { params: new HttpParams().set('hostname', hostname) } : {};
    return this.http.get<InventoryNode[]>(`${this.url}/inventory`, options);
  }

  inventoryDeviceList(hostname?: string): Observable<InventoryDevice[]> {
    return this.inventoryList(hostname).pipe(
      mergeMap((nodes: InventoryNode[]) => {
        const devices = _.flatMap(nodes, (node) => {
          return node.devices.map((device) => {
            device.hostname = node.name;
            device.uid = device.device_id ? device.device_id : `${device.hostname}-${device.path}`;
            return device;
          });
        });
        return observableOf(devices);
      })
    );
  }

  serviceList(hostname?: string) {
    const options = hostname ? { params: new HttpParams().set('hostname', hostname) } : {};
    return this.http.get(`${this.url}/service`, options);
  }

  osdCreate(driveGroup: {}) {
    const request = {
      drive_group: driveGroup
    };
    return this.http.post(`${this.url}/osd`, request, { observe: 'response' });
  }
}
