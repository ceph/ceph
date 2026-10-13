export interface NFSBackend {
  hostname: string;
  ip: string;
  port?: number;
  status?: string;
}

export type NFSHostPattern = string | { pattern: string; pattern_type?: string };

export interface NFSClusterPlacement {
  hosts?: string[];
  count?: number;
  label?: string;
  host_pattern?: NFSHostPattern;
}

export interface NFSCluster {
  name: string;
  virtual_ip?: string | number | null;
  port?: number;
  monitor_port?: number;
  backend: NFSBackend[];
  deployment_type?: string;
  ingress_mode?: string;
  placement?: NFSClusterPlacement;
  enable_rdma?: boolean;
  enable_nfsv3?: boolean;
}

export interface NFSBwIopConfig {
  cluster_id?: string;
  qos_type?: string;
  max_export_write_bw?: number;
  max_export_read_bw?: number;
  max_client_write_bw?: number;
  max_client_read_bw?: number;
  max_export_combined_bw?: number;
  max_client_combined_bw?: number;
  disable_qos?: boolean;
  enable_qos?: boolean;
  enable_bw_control?: boolean;
  combined_rw_bw_control?: boolean;
  pseudo_path?: string;
  max_export_iops?: number;
  max_client_iops?: number;
  disable_ops?: boolean;
  enable_ops?: boolean;
  disable_qos_ops?: boolean;
  enable_iops_control?: boolean;
}

export enum QOSType {
  PerShare = 'PerShare',
  PerClient = 'PerClient',
  PerSharePerClient = 'PerShare_PerClient'
}

export interface QOSTypeItem {
  key: string;
  value: string;
  help: string;
}

export interface BwTypeItem {
  value: string;
  help: string;
}

export enum NFS_TYPE {
  export = 'export',
  cluster = 'cluster'
}

export enum RateLimitType {
  Bandwidth = 'bandwidth',
  Iops = 'iops'
}

export const BW_UNITS: string[] = ['KiB/s', 'MiB/s', 'GiB/s'];

export const DEFAULT_BW_UNIT = 'MiB/s';

export const UNIT_TO_BYTES: { [key: string]: number } = {
  'KiB/s': 1024,
  'MiB/s': 1024 ** 2,
  'GiB/s': 1024 ** 3
};

export const MIN_BW_BYTES = 32 * 1024;

export const MAX_BW_BYTES = 100 * 1024 ** 3;
