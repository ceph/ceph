import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import _ from 'lodash';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import { CdDevice } from '../models/devices';
import { InventoryDeviceType } from '../models/inventory-device-type.model';
import { DeploymentOptions } from '../models/osd-deployment-options';
import { OsdSettings } from '../models/osd-settings';
import { SmartDataResponseV1 } from '../models/smart';
import { DeviceService } from '../services/device.service';
import { CdFormGroup } from '../forms/cd-form-group';

@Injectable({
  providedIn: 'root'
})
export class OsdService {
  private path = 'api/osd';
  private uiPath = 'ui-api/osd';

  osdDevices: InventoryDeviceType[] = [];
  selectedFormValues: CdFormGroup;
  isDeployementModeSimple: boolean = true;

  osdRecvSpeedModalPriorities = {
    KNOWN_PRIORITIES: [
      {
        name: null,
        text: $localize`-- Select the priority --`,
        values: {
          osd_max_backfills: null,
          osd_recovery_max_active: null,
          osd_recovery_max_single_start: null,
          osd_recovery_sleep: null
        }
      },
      {
        name: 'low',
        text: $localize`Low`,
        values: {
          osd_max_backfills: 1,
          osd_recovery_max_active: 1,
          osd_recovery_max_single_start: 1,
          osd_recovery_sleep: 0.5
        }
      },
      {
        name: 'default',
        text: $localize`Default`,
        values: {
          osd_max_backfills: 1,
          osd_recovery_max_active: 3,
          osd_recovery_max_single_start: 1,
          osd_recovery_sleep: 0
        }
      },
      {
        name: 'high',
        text: $localize`High`,
        values: {
          osd_max_backfills: 4,
          osd_recovery_max_active: 4,
          osd_recovery_max_single_start: 4,
          osd_recovery_sleep: 0
        }
      }
    ]
  };

  constructor(private http: HttpClient, private deviceService: DeviceService) {}

  create(driveGroups: Object[], trackingId: string, method = 'drive_groups') {
    const request = {
      method: method,
      data: driveGroups,
      tracking_id: trackingId
    };
    return this.http.post(this.path, request, { observe: 'response' });
  }

  getList() {
    return this.http.get(`${this.path}`);
  }

  getOsdSettings(): Observable<OsdSettings> {
    return this.http.get<OsdSettings>(`${this.path}/settings`, {
      headers: { Accept: 'application/vnd.ceph.api.v0.1+json' }
    });
  }

  getDetails(id: number) {
    interface OsdData {
      osd_map: { [key: string]: any };
      osd_metadata: { [key: string]: any };
      smart: { [device_identifier: string]: any };
    }
    return this.http.get<OsdData>(`${this.path}/${id}`);
  }

  /**
   * @param id OSD ID
   */
  getSmartData(id: number) {
    return this.http.get<SmartDataResponseV1>(`${this.path}/${id}/smart`);
  }

  scrub(id: string, deep: boolean) {
    return this.http.post(`${this.path}/${id}/scrub?deep=${deep}`, null);
  }

  getDeploymentOptions() {
    return this.http.get<DeploymentOptions>(`${this.uiPath}/deployment_options`);
  }

  getFlags() {
    return this.http.get(`${this.path}/flags`);
  }

  updateFlags(flags: string[]) {
    return this.http.put(`${this.path}/flags`, { flags: flags });
  }

  updateIndividualFlags(flags: { [flag: string]: boolean }, ids: number[]) {
    return this.http.put(`${this.path}/flags/individual`, { flags: flags, ids: ids });
  }

  markOut(id: number) {
    return this.http.put(`${this.path}/${id}/mark`, { action: 'out' });
  }

  markIn(id: number) {
    return this.http.put(`${this.path}/${id}/mark`, { action: 'in' });
  }

  markDown(id: number) {
    return this.http.put(`${this.path}/${id}/mark`, { action: 'down' });
  }

  reweight(id: number, weight: number) {
    return this.http.post(`${this.path}/${id}/reweight`, { weight: weight });
  }

  update(id: number, deviceClass: string) {
    return this.http.put(`${this.path}/${id}`, { device_class: deviceClass });
  }

  markLost(id: number) {
    return this.http.put(`${this.path}/${id}/mark`, { action: 'lost' });
  }

  purge(id: number) {
    return this.http.post(`${this.path}/${id}/purge`, null);
  }

  destroy(id: number) {
    return this.http.post(`${this.path}/${id}/destroy`, null);
  }

  delete(id: number, preserveId?: boolean, force?: boolean) {
    const params = {
      preserve_id: preserveId ? 'true' : 'false',
      force: force ? 'true' : 'false'
    };
    return this.http.delete(`${this.path}/${id}`, { observe: 'response', params: params });
  }

  safeToDestroy(ids: string) {
    interface SafeToDestroyResponse {
      active: number[];
      missing_stats: number[];
      stored_pgs: number[];
      is_safe_to_destroy: boolean;
      message?: string;
    }
    return this.http.get<SafeToDestroyResponse>(`${this.path}/safe_to_destroy?ids=${ids}`);
  }

  safeToDelete(ids: string) {
    interface SafeToDeleteResponse {
      is_safe_to_delete: boolean;
      message?: string;
    }
    return this.http.get<SafeToDeleteResponse>(`${this.path}/safe_to_delete?svc_ids=${ids}`);
  }

  getDevices(osdId: number) {
    return this.http
      .get<CdDevice[]>(`${this.path}/${osdId}/devices`)
      .pipe(map((devices) => devices.map((device) => this.deviceService.prepareDevice(device))));
  }
}
