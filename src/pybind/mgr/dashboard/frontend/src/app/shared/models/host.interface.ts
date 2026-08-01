import { HostStatus } from '../enum/host-status.enum';
import { ICON_TYPE } from '../enum/icons.enum';

export type OverviewStatusIcon = 'danger' | 'info-circle' | 'success' | 'warning';
export interface Host {
  ceph_version: string;
  services: Array<{ type: string; id: string }>;
  sources: {
    ceph: boolean;
    orchestrator: boolean;
  };
  hostname: string;
  addr: string;
  labels: string[];
  status: any;
  service_instances: Array<{ type: string; count: number }>;
}

export interface HostOverviewDetails extends Host {
  model?: string;
  cpu_count?: number | string;
  cpu_cores?: number | string;
  memory_total_kb?: number | string;
  hdd_capacity_bytes?: number | string;
  flash_capacity_bytes?: number | string;
  hdd_count?: number | string;
  flash_count?: number | string;
  nic_count?: number | string;
}

export interface HostStatusConfig {
  status: HostStatus | string;
  icon: OverviewStatusIcon;
  label: string;
}

export const STATUS_MAP: Record<string, HostStatusConfig> = {
  available: {
    status: HostStatus.AVAILABLE,
    icon: ICON_TYPE.success,
    label: $localize`Available`
  },
  maintenance: {
    status: HostStatus.MAINTENANCE,
    icon: ICON_TYPE.warning,
    label: $localize`Maintenance`
  },
  offline: {
    status: HostStatus.OFFLINE,
    icon: ICON_TYPE.danger,
    label: $localize`Offline`
  }
};

export function getStatus(host: { status?: string } = {}): string {
  const status = host?.status?.trim()?.toLowerCase();
  if (status === '') return 'available';
  return status;
}
