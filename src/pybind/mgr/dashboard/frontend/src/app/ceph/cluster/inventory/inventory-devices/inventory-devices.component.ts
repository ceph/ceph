import { Component, EventEmitter, Input, OnChanges, OnInit, Output } from '@angular/core';
import { I18n } from '@ngx-translate/i18n-polyfill';

import { getterForProp } from '@swimlane/ngx-datatable/release/utils';
import * as _ from 'lodash';

import { CellTemplate } from '../../../../shared/enum/cell-template.enum';
import { Icons } from '../../../../shared/enum/icons.enum';
import { CdTableColumn } from '../../../../shared/models/cd-table-column';
import { DimlessBinaryPipe } from '../../../../shared/pipes/dimless-binary.pipe';
import { InventoryDeviceFilter } from './inventory-device-filter.interface';
import { InventoryDeviceFiltersChangeEvent } from './inventory-device-filters-change-event.interface';
import { InventoryDevice } from './inventory-device.model';

@Component({
  selector: 'cd-inventory-devices',
  templateUrl: './inventory-devices.component.html',
  styleUrls: ['./inventory-devices.component.scss']
})
export class InventoryDevicesComponent implements OnInit, OnChanges {
  // Devices
  @Input() devices: InventoryDevice[] = [];

  // Do not display these columns
  @Input() hiddenColumns: string[] = [];

  // Show filters for these columns, specify empty array to disable
  @Input() filterColumns = [
    'hostname',
    'human_readable_type',
    'available',
    'sys_api.vendor',
    'sys_api.model',
    'sys_api.size'
  ];

  // Device table row selection type
  @Input() selectionType: string = undefined;

  @Output() filterChange = new EventEmitter<InventoryDeviceFiltersChangeEvent>();

  filterInDevices: InventoryDevice[] = [];
  filterOutDevices: InventoryDevice[] = [];

  icons = Icons;
  columns: Array<CdTableColumn> = [];
  filters: InventoryDeviceFilter[] = [];

  constructor(private dimlessBinary: DimlessBinaryPipe, private i18n: I18n) {}

  ngOnInit() {
    const columns = [
      {
        name: this.i18n('Hostname'),
        prop: 'hostname',
        flexGrow: 1
      },
      {
        name: this.i18n('Device path'),
        prop: 'path',
        flexGrow: 1
      },
      {
        name: this.i18n('Type'),
        prop: 'human_readable_type',
        flexGrow: 1,
        cellClass: 'text-center',
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          map: {
            hdd: { value: 'HDD', class: 'badge-hdd' },
            'ssd/nvme': { value: 'SSD', class: 'badge-ssd' }
          }
        }
      },
      {
        name: this.i18n('Available'),
        prop: 'available',
        flexGrow: 1
      },
      {
        name: this.i18n('Vendor'),
        prop: 'sys_api.vendor',
        flexGrow: 1
      },
      {
        name: this.i18n('Model'),
        prop: 'sys_api.model',
        flexGrow: 1
      },
      {
        name: this.i18n('Size'),
        prop: 'sys_api.size',
        flexGrow: 1,
        pipe: this.dimlessBinary
      },
      {
        name: this.i18n('OSDs'),
        prop: 'osd_ids',
        flexGrow: 1,
        cellClass: 'text-center',
        cellTransformation: CellTemplate.badge,
        customTemplateConfig: {
          class: 'badge-dark',
          prefix: 'osd.'
        }
      }
    ];

    this.columns = columns.filter((col: any) => {
      return !this.hiddenColumns.includes(col.prop);
    });

    // init filters
    this.filters = this.columns
      .filter((col: any) => {
        return this.filterColumns.includes(col.prop);
      })
      .map((col: any) => {
        return {
          label: col.name,
          prop: col.prop,
          initValue: '*',
          value: '*',
          options: [{ value: '*', formatValue: '*' }],
          pipe: col.pipe
        };
      });

    this.filterInDevices = [...this.devices];
    this.updateFilterOptions(this.devices);
  }

  ngOnChanges() {
    this.updateFilterOptions(this.devices);
    this.filterInDevices = [...this.devices];
    // TODO: apply filter, columns changes, filter changes
  }

  updateFilterOptions(devices: InventoryDevice[]) {
    // update filter options to all possible values in a column, might be time-consuming
    this.filters.forEach((filter) => {
      const values = _.sortedUniq(_.map(devices, filter.prop).sort());
      const options = values.map((v: string) => {
        return {
          value: v,
          formatValue: filter.pipe ? filter.pipe.transform(v) : v
        };
      });
      filter.options = [{ value: '*', formatValue: '*' }, ...options];
    });
  }

  doFilter() {
    this.filterOutDevices = [];
    const appliedFilters = [];
    let devices: any = [...this.devices];
    this.filters.forEach((filter) => {
      if (filter.value === filter.initValue) {
        return;
      }
      appliedFilters.push({
        label: filter.label,
        prop: filter.prop,
        value: filter.value,
        formatValue: filter.pipe ? filter.pipe.transform(filter.value) : filter.value
      });
      // Separate devices to filter-in and filter-out parts.
      // Cast column value to string type because options are always string.
      const parts = _.partition(devices, (row) => {
        // use getter from ngx-datatable for props like 'sys_api.size'
        const valueGetter = getterForProp(filter.prop);
        return `${valueGetter(row, filter.prop)}` === filter.value;
      });
      devices = parts[0];
      this.filterOutDevices = [...this.filterOutDevices, ...parts[1]];
    });
    this.filterInDevices = devices;
    this.filterChange.emit({
      filters: appliedFilters,
      filterInDevices: this.filterInDevices,
      filterOutDevices: this.filterOutDevices
    });
  }

  onFilterChange() {
    this.doFilter();
  }

  onFilterReset() {
    this.filters.forEach((item) => {
      item.value = item.initValue;
    });
    this.filterInDevices = [...this.devices];
    this.filterOutDevices = [];
    this.filterChange.emit({
      filters: [],
      filterInDevices: this.filterInDevices,
      filterOutDevices: this.filterOutDevices
    });
  }
}
