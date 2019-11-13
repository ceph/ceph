export class SysAPI {
  vendor: string;
  model: string;
  size: number;
  rotational: string;
  human_readable_size: string;
}

export class Device {
  hostname: string;
  uid: string;
  osd_ids: number[];

  path: string;
  sys_api: SysAPI;
  available: boolean;
  rejected_reasons: string[];
  device_id: string;
  human_readable_type: string;
}

export class InventoryNode {
  name: string;
  devices: Device[];
}
