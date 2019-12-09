import { Component, Input, OnChanges, OnInit, SimpleChanges } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';
import * as _ from 'lodash';
import { HostService } from '../../../shared/api/host.service';
import {
  OsdService,
  SmartAttribute,
  SmartDataV1,
  SmartError
} from '../../../shared/api/osd.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';

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

  constructor(
    private i18n: I18n,
    private osdService: OsdService,
    private hostService: HostService
  ) {}

  private isSmartError(data: SmartDataV1 | SmartError): data is SmartError {
    return (data as SmartError).error !== undefined;
  }

  private fetchData(data) {
    const result = {};
    _.each(data, (smartData, deviceId) => {
      if (this.isSmartError(smartData)) {
        let userMessage = '';
        if (smartData.smartctl_error_code === -22) {
          userMessage = this.i18n(
            'Smartctl has received an unknown argument (error code ' +
              '{{code}}). You may be using an ' +
              'incompatible version of smartmontools. Version >= 7.0 of ' +
              'smartmontools is required to successfully retrieve data.',
            { code: smartData.smartctl_error_code }
          );
        } else {
          userMessage = this.i18n('An error with error code {{code}} occurred.', {
            code: smartData.smartctl_error_code
          });
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
          smart: {
            attributes: smartData.ata_smart_attributes,
            data: smartData.ata_smart_data
          },
          device: info.device.name,
          identifier: info.serial_number
        };
      } else {
        this.incompatible = true;
      }
    });
    this.data = result;
    this.loading = false;
  }

  private updateData() {
    this.loading = true;

    if (this.osdId !== null) {
      this.osdService.getSmartData(this.osdId).subscribe(this.fetchData.bind(this));
    } else if (this.hostname !== null) {
      this.hostService.getSmartData(this.hostname).subscribe(this.fetchData.bind(this));
    }
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
    if (changes.osdId) {
      this.osdId = changes.osdId.currentValue;
    } else if (changes.hostname) {
      this.hostname = changes.hostname.currentValue;
    }
    this.updateData();
  }
}
