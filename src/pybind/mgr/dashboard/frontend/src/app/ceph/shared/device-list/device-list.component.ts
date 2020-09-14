import { DatePipe } from '@angular/common';
import { Component, Input, OnChanges, OnInit, TemplateRef, ViewChild } from '@angular/core';

import { HostService } from '../../../shared/api/host.service';
import { OsdService } from '../../../shared/api/osd.service';
import { CellTemplate } from '../../../shared/enum/cell-template.enum';
import { CdTableColumn } from '../../../shared/models/cd-table-column';
import { CdDevice } from '../../../shared/models/devices';

@Component({
  selector: 'cd-device-list',
  templateUrl: './device-list.component.html',
  styleUrls: ['./device-list.component.scss']
})
export class DeviceListComponent implements OnChanges, OnInit {
  @Input()
  hostname = '';
  @Input()
  osdId: number = null;

  @ViewChild('deviceLocation', { static: true })
  locationTemplate: TemplateRef<any>;
  @ViewChild('lifeExpectancy', { static: true })
  lifeExpectancyTemplate: TemplateRef<any>;
  @ViewChild('lifeExpectancyTimestamp', { static: true })
  lifeExpectancyTimestampTemplate: TemplateRef<any>;

  devices: CdDevice[] = null;
  columns: CdTableColumn[] = [];
  translationMapping = {
    '=1': '# week',
    other: '# weeks'
  };

  constructor(
    private hostService: HostService,
    private datePipe: DatePipe,
    private osdService: OsdService
  ) {}

  ngOnInit() {
    this.columns = [
      { prop: 'devid', name: $localize`Device ID`, minWidth: 200 },
      {
        prop: 'state',
        name: $localize`State of Health`,
        flexGrow: 1,
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            good: { value: $localize`Good`, class: 'badge-success' },
            warning: { value: $localize`Warning`, class: 'badge-warning' },
            bad: { value: $localize`Bad`, class: 'badge-danger' },
            stale: { value: $localize`Stale`, class: 'badge-info' },
            unknown: { value: $localize`Unknown`, class: 'badge-dark' }
          }
        }
      },
      {
        prop: 'life_expectancy_weeks',
        name: $localize`Life Expectancy`,
        cellTemplate: this.lifeExpectancyTemplate
      },
      {
        prop: 'life_expectancy_stamp',
        name: $localize`Prediction Creation Date`,
        cellTemplate: this.lifeExpectancyTimestampTemplate,
        pipe: this.datePipe,
        isHidden: true
      },
      { prop: 'location', name: $localize`Device Name`, cellTemplate: this.locationTemplate },
      { prop: 'readableDaemons', name: $localize`Daemons` }
    ];
  }

  ngOnChanges() {
    const updateDevicesFn = (devices: CdDevice[]) => (this.devices = devices);
    if (this.hostname) {
      this.hostService.getDevices(this.hostname).subscribe(updateDevicesFn);
    } else if (this.osdId !== null) {
      this.osdService.getDevices(this.osdId).subscribe(updateDevicesFn);
    }
  }
}
