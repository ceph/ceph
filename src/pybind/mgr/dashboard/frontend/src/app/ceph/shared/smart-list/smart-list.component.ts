import { Component, Input, OnChanges, OnInit, SimpleChanges, ViewChild } from '@angular/core';

import { NgbNav } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { HostService } from '~/app/shared/api/host.service';
import { OsdService } from '~/app/shared/api/osd.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import {
  AtaSmartDataV1,
  IscsiSmartDataV1,
  NvmeSmartDataV1,
  SmartDataResult,
  SmartError,
  SmartErrorResult
} from '~/app/shared/models/smart';

@Component({
  selector: 'cd-smart-list',
  templateUrl: './smart-list.component.html',
  styleUrls: ['./smart-list.component.scss']
})
export class SmartListComponent implements OnInit, OnChanges {
  @ViewChild('innerNav')
  nav: NgbNav;

  @Input()
  osdId: number = null;
  @Input()
  hostname: string = null;

  loading = false;
  incompatible = false;
  error = false;

  data: { [deviceId: string]: SmartDataResult | SmartErrorResult } = {};

  smartDataColumns: CdTableColumn[];
  scsiSmartDataColumns: CdTableColumn[];

  isEmpty = _.isEmpty;

  constructor(private osdService: OsdService, private hostService: HostService) {}

  isSmartError(data: any): data is SmartError {
    return _.get(data, 'error') !== undefined;
  }

  isNvmeSmartData(data: any): data is NvmeSmartDataV1 {
    return _.get(data, 'device.protocol', '').toLowerCase() === 'nvme';
  }

  isAtaSmartData(data: any): data is AtaSmartDataV1 {
    return _.get(data, 'device.protocol', '').toLowerCase() === 'ata';
  }

  isIscsiSmartData(data: any): data is IscsiSmartDataV1 {
    return _.get(data, 'device.protocol', '').toLowerCase() === 'scsi';
  }

  private fetchData(data: any) {
    const result: { [deviceId: string]: SmartDataResult | SmartErrorResult } = {};
    _.each(data, (smartData, deviceId) => {
      if (this.isSmartError(smartData)) {
        let userMessage = '';
        if (smartData.smartctl_error_code === -22) {
          userMessage = $localize`Smartctl has received an unknown argument \
(error code ${smartData.smartctl_error_code}). \
You may be using an incompatible version of smartmontools. Version >= 7.0 of \
smartmontools is required to successfully retrieve data.`;
        } else {
          userMessage = $localize`An error with error code ${smartData.smartctl_error_code} occurred.`;
        }
        const _result: SmartErrorResult = {
          error: smartData.error,
          smartctl_error_code: smartData.smartctl_error_code,
          smartctl_output: smartData.smartctl_output,
          userMessage: userMessage,
          device: smartData.dev,
          identifier: smartData.nvme_vendor
        };
        result[deviceId] = _result;
        return;
      }
      // Prepare S.M.A.R.T data
      if (smartData.json_format_version[0] === 1) {
        // Version 1.x
        if (this.isAtaSmartData(smartData)) {
          result[deviceId] = this.extractAtaData(smartData);
        } else if (this.isIscsiSmartData(smartData)) {
          result[deviceId] = this.extractIscsiData(smartData);
        } else if (this.isNvmeSmartData(smartData)) {
          result[deviceId] = this.extractNvmeData(smartData);
        }
        return;
      } else {
        this.incompatible = true;
      }
    });
    this.data = result;
    this.loading = false;
  }

  private extractNvmeData(smartData: NvmeSmartDataV1): SmartDataResult {
    const info = _.omitBy(smartData, (_value: string, key: string) =>
      ['nvme_smart_health_information_log'].includes(key)
    );
    return {
      info: info,
      smart: {
        nvmeData: smartData.nvme_smart_health_information_log
      },
      device: smartData.device.name,
      identifier: smartData.serial_number
    };
  }

  private extractIscsiData(smartData: IscsiSmartDataV1): SmartDataResult {
    const info = _.omitBy(smartData, (_value: string, key: string) =>
      ['scsi_error_counter_log', 'scsi_grown_defect_list'].includes(key)
    );
    return {
      info: info,
      smart: {
        scsi_error_counter_log: smartData.scsi_error_counter_log,
        scsi_grown_defect_list: smartData.scsi_grown_defect_list
      },
      device: info.device.name,
      identifier: info.serial_number
    };
  }

  private extractAtaData(smartData: AtaSmartDataV1): SmartDataResult {
    const info = _.omitBy(smartData, (_value: string, key: string) =>
      ['ata_smart_attributes', 'ata_smart_selective_self_test_log', 'ata_smart_data'].includes(key)
    );
    return {
      info: info,
      smart: {
        attributes: smartData.ata_smart_attributes,
        data: smartData.ata_smart_data
      },
      device: info.device.name,
      identifier: info.serial_number
    };
  }

  private updateData() {
    this.loading = true;

    if (this.osdId !== null) {
      this.osdService.getSmartData(this.osdId).subscribe({
        next: this.fetchData.bind(this),
        error: (error) => {
          error.preventDefault();
          this.error = error;
          this.loading = false;
        }
      });
    } else if (this.hostname !== null) {
      this.hostService.getSmartData(this.hostname).subscribe({
        next: this.fetchData.bind(this),
        error: (error) => {
          error.preventDefault();
          this.error = error;
          this.loading = false;
        }
      });
    }
  }

  ngOnInit() {
    this.smartDataColumns = [
      { prop: 'id', name: $localize`ID` },
      { prop: 'name', name: $localize`Name` },
      { prop: 'raw.value', name: $localize`Raw` },
      { prop: 'thresh', name: $localize`Threshold` },
      { prop: 'value', name: $localize`Value` },
      { prop: 'when_failed', name: $localize`When Failed` },
      { prop: 'worst', name: $localize`Worst` }
    ];

    this.scsiSmartDataColumns = [
      {
        prop: 'correction_algorithm_invocations',
        name: $localize`Correction Algorithm Invocations`
      },
      {
        prop: 'errors_corrected_by_eccdelayed',
        name: $localize`Errors Corrected by ECC (Delayed)`
      },
      { prop: 'errors_corrected_by_eccfast', name: $localize`Errors Corrected by ECC (Fast)` },
      {
        prop: 'errors_corrected_by_rereads_rewrites',
        name: $localize`Errors Corrected by Rereads/Rewrites`
      },
      { prop: 'gigabytes_processed', name: $localize`Gigabytes Processed` },
      { prop: 'total_errors_corrected', name: $localize`Total Errors Corrected` },
      { prop: 'total_uncorrected_errors', name: $localize`Total Errors Uncorrected` }
    ];
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.data = {}; // Clear previous data
    if (changes.osdId) {
      this.osdId = changes.osdId.currentValue;
    } else if (changes.hostname) {
      this.hostname = changes.hostname.currentValue;
    }
    this.updateData();
  }
}
