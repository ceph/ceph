import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { Daemon } from '../models/daemon.interface';
import { CdDevice } from '../models/devices';
import { SmartDataResponseV1 } from '../models/smart';
import { DeviceService } from '../services/device.service';

@Injectable({
  providedIn: 'root'
})
export class HostService {
  baseURL = 'api/host';

  constructor(private http: HttpClient, private deviceService: DeviceService) {}

  list() {
    return this.http.get(this.baseURL);
  }

  create(hostname: string) {
    return this.http.post(this.baseURL, { hostname: hostname }, { observe: 'response' });
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
    return this.http.get<string[]>('ui-api/host/labels');
  }

  update(hostname: string, labels: string[]) {
    return this.http.put(`${this.baseURL}/${hostname}`, { labels: labels });
  }
}
