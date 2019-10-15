import { InventoryDeviceFiltersChangeEvent } from '../../inventory/inventory-devices/inventory-device-filters-change-event.interface';

export interface DevicesSelectionChangeEvent extends InventoryDeviceFiltersChangeEvent {
  type: string;
}
