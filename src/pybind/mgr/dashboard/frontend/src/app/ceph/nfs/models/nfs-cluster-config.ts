export interface NFSCluster {
  name: string;
  virtual_ip: number;
  port: number;
  backend: NFSBackend[];
}
export interface NFSBackend {
  hostname: string;
  ip: string;
  port: number;
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

export interface bwTypeItem {
  value: string;
  help: string;
}
