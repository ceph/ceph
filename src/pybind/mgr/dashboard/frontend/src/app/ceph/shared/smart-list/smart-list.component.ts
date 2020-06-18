import { Component, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';

import { HostService } from '../../../shared/api/host.service';
import { OsdService } from '../../../shared/api/osd.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import {
  HddSmartDataV1,
  NvmeSmartDataV1,
  SmartDataResult,
  SmartError,
  SmartErrorResult
} from '../../../shared/models/smart';

@Component({
  selector: 'cd-smart-list',
  templateUrl: './smart-list.component.html',
  styleUrls: ['./smart-list.component.scss']
})
export class SmartListComponent implements OnInit, OnChanges {
  @Input()
  osdId: number = null;
  @Input()
  hostname: string = null;

  loading = false;
  incompatible = false;
  error = false;

  data: { [deviceId: string]: SmartDataResult | SmartErrorResult } = {};

  smartDataColumns: CdTableColumn[];

  constructor(
    private i18n: I18n,
    private osdService: OsdService,
    private hostService: HostService
  ) {}

  isSmartError(data: any): data is SmartError {
    return _.get(data, 'error') !== undefined;
  }

  isNvmeSmartData(data: any): data is NvmeSmartDataV1 {
    return _.get(data, 'device.protocol', '').toLowerCase() === 'nvme';
  }

  isHddSmartData(data: any): data is HddSmartDataV1 {
    return _.get(data, 'device.protocol', '').toLowerCase() === 'ata';
  }

  private fetchData(data: any) {
    const result: { [deviceId: string]: SmartDataResult | SmartErrorResult } = {};
    _.each(data, (smartData, deviceId) => {
      if (this.isSmartError(smartData)) {
        let userMessage = '';
        if (smartData.smartctl_error_code === -22) {
          userMessage = this.i18n(
            `Smartctl has received an unknown argument (error code {{code}}). \
You may be using an incompatible version of smartmontools. Version >= 7.0 of \
smartmontools is required to successfully retrieve data.`,
            { code: smartData.smartctl_error_code }
          );
        } else {
          userMessage = this.i18n('An error with error code {{code}} occurred.', {
            code: smartData.smartctl_error_code
          });
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
        if (this.isHddSmartData(smartData)) {
          result[deviceId] = this.extractHddData(smartData);
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
    const info = _.omitBy(smartData, (_value, key) =>
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

  private extractHddData(smartData: HddSmartDataV1): SmartDataResult {
    const info = _.omitBy(smartData, (_value, key) =>
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
      { prop: 'id', name: this.i18n('ID') },
      { prop: 'name', name: this.i18n('Name') },
      { prop: 'raw.value', name: this.i18n('Raw') },
      { prop: 'thresh', name: this.i18n('Threshold') },
      { prop: 'value', name: this.i18n('Value') },
      { prop: 'when_failed', name: this.i18n('When Failed') },
      { prop: 'worst', name: this.i18n('Worst') }
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
