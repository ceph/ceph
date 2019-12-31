import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import { map } from 'rxjs/operators';

import { CdDevice } from '../models/devices';
import { DeviceService } from '../services/device.service';
import { ApiModule } from './api.module';

export interface SmartAttribute {
  flags: {
    auto_keep: boolean;
    error_rate: boolean;
    event_count: boolean;
    performance: boolean;
    prefailure: boolean;
    string: string;
    updated_online: boolean;
    value: number;
  };
  id: number;
  name: string;
  raw: { string: string; value: number };
  thresh: number;
  value: number;
  when_failed: string;
  worst: number;
}

export interface SmartError {
  dev: string;
  error: string;
  nvme_smart_health_information_add_log_error: string;
  nvme_smart_health_information_add_log_error_code: number;
  nvme_vendor: string;
  smartctl_error_code: number;
  smartctl_output: string;
}

export interface SmartDataV1 {
  ata_sct_capabilities: {
    data_table_supported: boolean;
    error_recovery_control_supported: boolean;
    feature_control_supported: boolean;
    value: number;
  };
  ata_smart_attributes: {
    revision: number;
    table: SmartAttribute[];
  };
  ata_smart_data: {
    capabilities: {
      attribute_autosave_enabled: boolean;
      conveyance_self_test_supported: boolean;
      error_logging_supported: boolean;
      exec_offline_immediate_supported: boolean;
      gp_logging_supported: boolean;
      offline_is_aborted_upon_new_cmd: boolean;
      offline_surface_scan_supported: boolean;
      selective_self_test_supported: boolean;
      self_tests_supported: boolean;
      values: number[];
    };
    offline_data_collection: {
      completion_seconds: number;
      status: { string: string; value: number };
    };
    self_test: {
      polling_minutes: { conveyance: number; extended: number; short: number };
      status: { passed: boolean; string: string; value: number };
    };
  };
  ata_smart_error_log: { summary: { count: number; revision: number } };
  ata_smart_selective_self_test_log: {
    flags: { remainder_scan_enabled: boolean; value: number };
    power_up_scan_resume_minutes: number;
    revision: number;
    table: {
      lba_max: number;
      lba_min: number;
      status: { string: string; value: number };
    }[];
  };
  ata_smart_self_test_log: { standard: { count: number; revision: number } };
  ata_version: { major_value: number; minor_value: number; string: string };
  device: { info_name: string; name: string; protocol: string; type: string };
  firmware_version: string;
  in_smartctl_database: boolean;
  interface_speed: {
    current: {
      bits_per_unit: number;
      sata_value: number;
      string: string;
      units_per_second: number;
    };
    max: {
      bits_per_unit: number;
      sata_value: number;
      string: string;
      units_per_second: number;
    };
  };
  json_format_version: number[];
  local_time: { asctime: string; time_t: number };
  logical_block_size: number;
  model_family: string;
  model_name: string;
  nvme_smart_health_information_add_log_error: string;
  nvme_smart_health_information_add_log_error_code: number;
  nvme_vendor: string;
  physical_block_size: number;
  power_cycle_count: number;
  power_on_time: { hours: number };
  rotation_rate: number;
  sata_version: { string: string; value: number };
  serial_number: string;
  smart_status: { passed: boolean };
  smartctl: {
    argv: string[];
    build_info: string;
    exit_status: number;
    output: string[];
    platform_info: string;
    svn_revision: string;
    version: number[];
  };
  temperature: { current: number };
  user_capacity: { blocks: number; bytes: number };
  wwn: { id: number; naa: number; oui: number };
}

@Injectable({
  providedIn: ApiModule
})
export class OsdService {
  private path = 'api/osd';

  osdRecvSpeedModalPriorities = {
    KNOWN_PRIORITIES: [
      {
        name: null,
        text: this.i18n('-- Select the priority --'),
        values: {
          osd_max_backfills: null,
          osd_recovery_max_active: null,
          osd_recovery_max_single_start: null,
          osd_recovery_sleep: null
        }
      },
      {
        name: 'low',
        text: this.i18n('Low'),
        values: {
          osd_max_backfills: 1,
          osd_recovery_max_active: 1,
          osd_recovery_max_single_start: 1,
          osd_recovery_sleep: 0.5
        }
      },
      {
        name: 'default',
        text: this.i18n('Default'),
        values: {
          osd_max_backfills: 1,
          osd_recovery_max_active: 3,
          osd_recovery_max_single_start: 1,
          osd_recovery_sleep: 0
        }
      },
      {
        name: 'high',
        text: this.i18n('High'),
        values: {
          osd_max_backfills: 4,
          osd_recovery_max_active: 4,
          osd_recovery_max_single_start: 4,
          osd_recovery_sleep: 0
        }
      }
    ]
  };

  constructor(private http: HttpClient, private i18n: I18n, private deviceService: DeviceService) {}

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
    return this.http.get<{ [deviceId: string]: SmartDataV1 | SmartError }>(
      `${this.path}/${id}/smart`
    );
  }

  scrub(id, deep) {
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

  safeToDestroy(ids: string) {
    interface SafeToDestroyResponse {
      'safe-to-destroy': boolean;
      message?: string;
    }
    return this.http.get<SafeToDestroyResponse>(`${this.path}/safe_to_destroy?ids=${ids}`);
  }

  getDevices(osdId: number) {
    return this.http
      .get<CdDevice[]>(`${this.path}/${osdId}/devices`)
      .pipe(map((devices) => devices.map((device) => this.deviceService.prepareDevice(device))));
  }
}
