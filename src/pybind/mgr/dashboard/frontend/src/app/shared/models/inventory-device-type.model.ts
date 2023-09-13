import { InventoryDevice } from '~/app/ceph/cluster/inventory/inventory-devices/inventory-device.model';

export interface InventoryDeviceType {
  type: string;
  capacity: number;
  devices: InventoryDevice[];
  canSelect: boolean;
  totalDevices: number;
}
