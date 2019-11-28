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
  statusURL = 'api/orchestrator/status';
  inventoryURL = 'api/orchestrator/inventory';
  serviceURL = 'api/orchestrator/service';
  osdURL = 'api/orchestrator/osd';

  constructor(private http: HttpClient) {}

  status() {
    return this.http.get(this.statusURL);
  }

  inventoryList(hostname?: string): Observable<InventoryNode[]> {
    const options = hostname ? { params: new HttpParams().set('hostname', hostname) } : {};
    return this.http.get<InventoryNode[]>(this.inventoryURL, options);
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
    return this.http.get(this.serviceURL, options);
  }

  osdCreate(driveGroup: {}, all_hosts: string[]) {
    const request = {
      drive_group: driveGroup
    };
    if (!_.isEmpty(all_hosts)) {
      request['all_hosts'] = all_hosts;
    }
    return this.http.post(this.osdURL, request, { observe: 'response' });
  }
}
