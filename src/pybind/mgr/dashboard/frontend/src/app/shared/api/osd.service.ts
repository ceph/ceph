import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import * as _ from 'lodash';
import { map } from 'rxjs/operators';

import { CdDevice } from '../models/devices';
import { SmartDataResponseV1 } from '../models/smart';
import { DeviceService } from '../services/device.service';

@Injectable({
  providedIn: 'root'
})
export class OsdService {
  private path = 'api/osd';

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

  create(driveGroups: Object[]) {
    const request = {
      method: 'drive_groups',
      data: driveGroups,
      tracking_id: _.join(_.map(driveGroups, 'service_id'), ', ')
    };
    return this.http.post(this.path, request, { observe: 'response' });
  }

  getList() {
    return this.http.get(`${this.path}`);
  }

  getDetails(id: number) {
    interface OsdData {
      osd_map: { [key: string]: any };
      osd_metadata: { [key: string]: any };
      histogram: { [key: string]: object };
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

  getFlags() {
    return this.http.get(`${this.path}/flags`);
  }

  updateFlags(flags: string[]) {
    return this.http.put(`${this.path}/flags`, { flags: flags });
  }

  markOut(id: number) {
    return this.http.post(`${this.path}/${id}/mark_out`, null);
  }

  markIn(id: number) {
    return this.http.post(`${this.path}/${id}/mark_in`, null);
  }

  markDown(id: number) {
    return this.http.post(`${this.path}/${id}/mark_down`, null);
  }

  reweight(id: number, weight: number) {
    return this.http.post(`${this.path}/${id}/reweight`, { weight: weight });
  }

  update(id: number, deviceClass: string) {
    return this.http.put(`${this.path}/${id}`, { device_class: deviceClass });
  }

  markLost(id: number) {
    return this.http.post(`${this.path}/${id}/mark_lost`, null);
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
