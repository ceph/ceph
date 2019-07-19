export interface Device {
  id: string;
  hostname: string;
  uid: string;
}

export interface InventoryNode {
  name: string;
  devices: Device[];
}
