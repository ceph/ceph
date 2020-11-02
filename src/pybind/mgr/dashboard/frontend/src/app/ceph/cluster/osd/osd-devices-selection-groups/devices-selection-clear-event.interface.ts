import { InventoryDevice } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-device.model';

export interface DevicesSelectionClearEvent {
  type: string;
  clearedDevices: InventoryDevice[];
}
