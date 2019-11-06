import { DatePipe } from '@angular/common';
import { Component, Input, OnInit, TemplateRef, ViewChild } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';
import { HostService } from '../../../shared/api/host.service';
import { OsdService } from '../../../shared/api/osd.service';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdDevice } from '../../../shared/models/devices';

@Component({
  selector: 'cd-device-list',
  templateUrl: './device-list.component.html',
  styleUrls: ['./device-list.component.scss']
})
export class DeviceListComponent implements OnInit {
  @Input()
  hostname = '';
  @Input()
  osdId = null;

  @ViewChild('deviceLocation', { static: true })
  locationTemplate: TemplateRef<any>;
  @ViewChild('lifeExpectancy', { static: true })
  lifeExpectancyTemplate: TemplateRef<any>;
  @ViewChild('lifeExpectancyTimestamp', { static: true })
  lifeExpectancyTimestampTemplate: TemplateRef<any>;
  @ViewChild('state', { static: true })
  stateTemplate: TemplateRef<any>;

  devices: CdDevice[] = null;
  columns: CdTableColumn[] = [];
  translationMapping = {
    '=1': '# week',
    other: '# weeks'
  };

  constructor(
    private hostService: HostService,
    private i18n: I18n,
    private datePipe: DatePipe,
    private osdService: OsdService
  ) {}

  ngOnInit() {
    const updateDevicesFn = (devices) => (this.devices = devices);
    if (this.hostname) {
      this.hostService.getDevices(this.hostname).subscribe(updateDevicesFn);
    } else if (this.osdId !== null) {
      this.osdService.getDevices(this.osdId).subscribe(updateDevicesFn);
    }
    this.columns = [
      { prop: 'devid', name: this.i18n('Device ID'), minWidth: 200 },
      {
        prop: 'state',
        name: this.i18n('State of Health'),
        cellTemplate: this.stateTemplate
      },
      {
        prop: 'life_expectancy_weeks',
        name: this.i18n('Life Expectancy'),
        cellTemplate: this.lifeExpectancyTemplate
      },
      {
        prop: 'life_expectancy_stamp',
        name: this.i18n('Prediction Creation Date'),
        cellTemplate: this.lifeExpectancyTimestampTemplate,
        pipe: this.datePipe,
        isHidden: true
      },
      { prop: 'location', name: this.i18n('Device Name'), cellTemplate: this.locationTemplate },
      { prop: 'readableDaemons', name: this.i18n('Daemons') }
    ];
  }
}
