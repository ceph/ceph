import { InventoryDeviceAppliedFilter } from './inventory-device-applied-filters.interface';
import { InventoryDevice } from './inventory-device.model';

export interface InventoryDeviceFiltersChangeEvent {
  filters: InventoryDeviceAppliedFilter[];
  filterInDevices: InventoryDevice[];
  filterOutDevices: InventoryDevice[];
}
