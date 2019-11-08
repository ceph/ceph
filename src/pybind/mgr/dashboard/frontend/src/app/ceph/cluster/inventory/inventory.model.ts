export class Device {
  hostname: string;
  uid: string;
  osd_ids: number[];

  blank: boolean;
  type: string;
  id: string;
  size: number;
  rotates: boolean;
  available: boolean;
  dev_id: string;
  extended: any;
}

export class InventoryNode {
  name: string;
  devices: Device[];
}
