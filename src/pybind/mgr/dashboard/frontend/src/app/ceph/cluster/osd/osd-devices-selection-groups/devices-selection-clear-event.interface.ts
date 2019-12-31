import { InventoryDevice } from '../../inventory/inventory-devices/inventory-device.model';

export interface DevicesSelectionClearEvent {
  type: string;
  clearedDevices: InventoryDevice[];
}
