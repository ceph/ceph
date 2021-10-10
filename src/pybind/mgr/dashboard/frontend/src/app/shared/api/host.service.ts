import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { Observable, of as observableOf } from 'rxjs';
import { map, mergeMap, toArray } from 'rxjs/operators';

import { Daemon } from '../models/daemon.interface';
import { CdDevice } from '../models/devices';
import { SmartDataResponseV1 } from '../models/smart';
import { DeviceService } from '../services/device.service';
import { CdHelperClass } from '~/app/shared/classes/cd-helper.class';
import { InventoryHost } from '~/app/ceph/cluster/inventory/inventory-host.model';
import { InventoryDevice } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-device.model';

@Injectable({
  providedIn: 'root'
})
export class HostService {
  baseURL = 'api/host';
  baseUIURL = 'ui-api/host';

  constructor(private http: HttpClient, private deviceService: DeviceService) {}

  list(): Observable<object[]> {
    return this.http.get<object[]>(this.baseURL);
  }

  create(hostname: string, addr: string, labels: string[], status: string) {
    return this.http.post(
      this.baseURL,
      { hostname, addr, labels, status },
      { observe: 'response', headers: { Accept: CdHelperClass.cdVersionHeader('0', '1') } }
    );
  }

  delete(hostname: string) {
    return this.http.delete(`${this.baseURL}/${hostname}`, { observe: 'response' });
  }

  getDevices(hostname: string): Observable<CdDevice[]> {
    return this.http
      .get<CdDevice[]>(`${this.baseURL}/${hostname}/devices`)
      .pipe(map((devices) => devices.map((device) => this.deviceService.prepareDevice(device))));
  }

  getSmartData(hostname: string) {
    return this.http.get<SmartDataResponseV1>(`${this.baseURL}/${hostname}/smart`);
  }

  getDaemons(hostname: string): Observable<Daemon[]> {
    return this.http.get<Daemon[]>(`${this.baseURL}/${hostname}/daemons`);
  }

  getLabels(): Observable<string[]> {
    return this.http.get<string[]>(`${this.baseUIURL}/labels`);
  }

  update(
    hostname: string,
    updateLabels = false,
    labels: string[] = [],
    maintenance = false,
    force = false
  ) {
    return this.http.put(
      `${this.baseURL}/${hostname}`,
      {
        update_labels: updateLabels,
        labels,
        maintenance,
        force
      },
      { headers: { Accept: 'application/vnd.ceph.api.v0.1+json' } }
    );
  }

  identifyDevice(hostname: string, device: string, duration: number) {
    return this.http.post(`${this.baseURL}/${hostname}/identify_device`, {
      device,
      duration
    });
  }

  private getInventoryParams(refresh?: boolean): HttpParams {
    let params = new HttpParams();
    if (refresh) {
      params = params.append('refresh', _.toString(refresh));
    }
    return params;
  }

  /**
   * Get inventory of a host.
   *
   * @param hostname the host query.
   * @param refresh true to ask the Orchestrator to refresh inventory.
   */
  getInventory(hostname: string, refresh?: boolean): Observable<InventoryHost> {
    const params = this.getInventoryParams(refresh);
    return this.http.get<InventoryHost>(`${this.baseURL}/${hostname}/inventory`, {
      params
    });
  }

  /**
   * Get inventories of all hosts.
   *
   * @param refresh true to ask the Orchestrator to refresh inventory.
   */
  inventoryList(refresh?: boolean): Observable<InventoryHost[]> {
    const params = this.getInventoryParams(refresh);
    return this.http.get<InventoryHost[]>(`${this.baseUIURL}/inventory`, { params });
  }

  /**
   * Get device list via host inventories.
   *
   * @param hostname the host to query. undefined for all hosts.
   * @param refresh true to ask the Orchestrator to refresh inventory.
   */
  inventoryDeviceList(hostname?: string, refresh?: boolean): Observable<InventoryDevice[]> {
    let observable;
    if (hostname) {
      observable = this.getInventory(hostname, refresh).pipe(toArray());
    } else {
      observable = this.inventoryList(refresh);
    }
    return observable.pipe(
      mergeMap((hosts: InventoryHost[]) => {
        const devices = _.flatMap(hosts, (host) =>
          host.devices.map((device) => {
            device.hostname = host.name;
            device.uid = device.device_id ? device.device_id : `${device.hostname}-${device.path}`;
            return device;
          })
        );
        return observableOf(devices);
      })
    );
  }
}
