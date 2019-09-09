import { Component, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import {
  OsdService,
  SmartAttribute,
  SmartDataV1,
  SmartError
} from '../../../../shared/api/osd.service';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';

@Component({
  selector: 'cd-osd-smart-list',
  templateUrl: './osd-smart-list.component.html',
  styleUrls: ['./osd-smart-list.component.scss']
})
export class OsdSmartListComponent implements OnInit, OnChanges {
  @Input()
  osdId: number;
  loading = false;
  incompatible = false;

  data: {
    [deviceId: string]: {
      info: { [key: string]: number | string | boolean };
      smart: {
        revision: number;
        table: SmartAttribute[];
      };
    };
  } = {};

  columns: CdTableColumn[];

  constructor(private i18n: I18n, private osdService: OsdService) {}

  private isSmartError(data: SmartDataV1 | SmartError): data is SmartError {
    return (data as SmartError).error !== undefined;
  }

  private updateData(osdId: number) {
    this.loading = true;
    this.osdService.getSmartData(osdId).subscribe((data) => {
      const result = {};
      _.each(data, (smartData, deviceId) => {
        if (this.isSmartError(smartData)) {
          let userMessage = '';
          if (smartData.smartctl_error_code === -22) {
            const msg =
              `Smartctl has received an unknown argument (error code ` +
              `${smartData.smartctl_error_code}). You may be using an incompatible version of ` +
              `smartmontools.  Version >= 7.0 of smartmontools is required to ` +
              `succesfully retrieve data.`;
            userMessage = this.i18n(msg);
          } else {
            const msg = `An error with error code ${smartData.smartctl_error_code} occurred.`;
            userMessage = this.i18n(msg);
          }
          result[deviceId] = {
            error: smartData.error,
            smartctl_error_code: smartData.smartctl_error_code,
            smartctl_output: smartData.smartctl_output,
            userMessage: userMessage,
            device: smartData.dev,
            identifier: smartData.nvme_vendor
          };
          return;
        }

        // Prepare S.M.A.R.T data
        if (smartData.json_format_version[0] === 1) {
          // Version 1.x
          const excludes = [
            'ata_smart_attributes',
            'ata_smart_selective_self_test_log',
            'ata_smart_data'
          ];
          const info = _.pickBy(smartData, (_value, key) => !excludes.includes(key));
          // Build result
          result[deviceId] = {
            info: info,
            smart: smartData.ata_smart_attributes,
            device: info.device.name,
            identifier: info.serial_number
          };
        } else {
          this.incompatible = true;
        }
      });
      this.data = result;
      this.loading = false;
    });
  }

  ngOnInit() {
    this.columns = [
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
    this.updateData(changes.osdId.currentValue);
  }
}
