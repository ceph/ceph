export interface DashboardDetails {
  fsid?: string;
  orchestrator?: string;
  cephVersion?: string;
}

export interface CapacityCardDetails {
  osdNearfull: number;
  osdFull: number;
}

export interface InventoryCommonDetail {
  total: number;
}

export interface InventoryDetails extends InventoryCommonDetail {
  info: number;
  success: number;
}
