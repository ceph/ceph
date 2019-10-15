import { FormatterService } from '../../../../shared/services/formatter.service';
import { InventoryDeviceAppliedFilter } from '../../inventory/inventory-devices/inventory-device-applied-filters.interface';

export class DriveGroup {
  // DriveGroupSpec object.
  spec = {};

  // Map from filter column prop to device selection attribute name
  private deviceSelectionAttrs: {
    [key: string]: {
      name: string;
      formatter?: Function;
    };
  };

  private formatterService: FormatterService;

  constructor() {
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
    this.spec = {};
  }

  setHostPattern(pattern: string) {
    this.spec['host_pattern'] = pattern;
  }

  setDeviceSelection(type: string, appliedFilters: InventoryDeviceAppliedFilter[]) {
    const key = `${type}_devices`;
    this.spec[key] = {};
    appliedFilters.forEach((filter) => {
      const attr = this.deviceSelectionAttrs[filter.prop];
      if (attr) {
        const name = attr.name;
        this.spec[key][name] = attr.formatter ? attr.formatter(filter.value) : filter.value;
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
