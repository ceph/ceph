import * as _ from 'lodash';

import { CdTableColumnFiltersChange } from '../../../../shared/models/cd-table-column-filters-change';
import { FormatterService } from '../../../../shared/services/formatter.service';

export class DriveGroup {
  spec: Object;

  // Map from filter column prop to device selection attribute name
  private deviceSelectionAttrs: {
    [key: string]: {
      name: string;
      formatter?: Function;
    };
  };

  private formatterService: FormatterService;

  constructor() {
    this.reset();
    this.formatterService = new FormatterService();
    this.deviceSelectionAttrs = {
      'sys_api.vendor': {
        name: 'vendor'
      },
      'sys_api.model': {
        name: 'model'
      },
      device_id: {
        name: 'device_id'
      },
      human_readable_type: {
        name: 'rotational',
        formatter: (value: string) => {
          return value.toLowerCase() === 'hdd';
        }
      },
      'sys_api.size': {
        name: 'size',
        formatter: (value: string) => {
          return this.formatterService
            .format_number(value, 1024, ['B', 'KB', 'MB', 'GB', 'TB', 'PB'])
            .replace(' ', '');
        }
      }
    };
  }

  reset() {
    this.spec = {
      service_type: 'osd',
      service_id: `dashboard-${_.now()}`
    };
  }

  setName(name: string) {
    this.spec['service_id'] = name;
  }

  setHostPattern(pattern: string) {
    this.spec['host_pattern'] = pattern;
  }

  setDeviceSelection(type: string, appliedFilters: CdTableColumnFiltersChange['filters']) {
    const key = `${type}_devices`;
    this.spec[key] = {};
    appliedFilters.forEach((filter) => {
      const attr = this.deviceSelectionAttrs[filter.prop];
      if (attr) {
        const name = attr.name;
        this.spec[key][name] = attr.formatter ? attr.formatter(filter.value.raw) : filter.value.raw;
      }
    });
  }

  clearDeviceSelection(type: string) {
    const key = `${type}_devices`;
    delete this.spec[key];
  }

  setSlots(type: string, slots: number) {
    const key = `${type}_slots`;
    if (slots === 0) {
      delete this.spec[key];
    } else {
      this.spec[key] = slots;
    }
  }

  setFeature(feature: string, enabled: boolean) {
    if (enabled) {
      this.spec[feature] = true;
    } else {
      delete this.spec[feature];
    }
  }
}
