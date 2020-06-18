import { InventoryDevice } from '../../ceph/cluster/inventory/inventory-devices/inventory-device.model';

export interface Data {
  human_readable_size: string;
  path: string;
  percentage: number;
  size: number;
  devices: InventoryDevice[];
  devices_names: string[];
}

export interface Vg {
  devices: InventoryDevice[];
  devices_names: string[];
  human_readable_size: string;
  human_readable_sizes: string;
  parts: number;
  percentages: number;
  size: number;
  sizes: number;
}

export interface Shared {
  human_readable_size: string;
  path: string;
  percentage: number;
  size: number;
  vg: Vg;
}

// The preview items to be displayed in Dashboard
export interface OSDPreview {
  id: number; // For uniqueness in table, not actual OSD ID.
  hostname: string;
  data: Data;
  wal: Shared;
  db: Shared;
}
