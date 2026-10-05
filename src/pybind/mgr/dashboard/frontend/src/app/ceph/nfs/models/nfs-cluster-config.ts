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
