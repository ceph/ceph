import { IngressMode } from '../nfs.constant';

export interface NFSBackend {
  hostname: string;
  ip: string;
  port?: number;
  status?: string;
}

export interface NFSClusterPlacement {
  hosts?: string[];
  count?: number;
  label?: string;
  host_pattern?: string;
}

export interface NFSCluster {
  name: string;
  virtual_ip: string | null;
  port?: number;
  backend: NFSBackend[];
  enable_nfsv3?: boolean;
  placement: NFSClusterPlacement;
  enable_rdma?: boolean;
  deployment_type?: string;
  ingress_mode?: IngressMode;
  monitor_port?: number;
  error?: string;
}

export interface NFSClusterOption {
  cluster_id: string;
  enable_rdma?: boolean;
}

export interface NFSShare {
  cluster_id?: string;
  export_id?: number;
}

export interface NFSBindAddr {
  hostname: string;
  ip: string;
}

export interface NFSClusterPayload {
  cluster_id: string;
  port?: number;
  placement?: NFSClusterPlacement;
  networks?: string[];
  bind_addrs?: NFSBindAddr[];
  ingress?: boolean;
  virtual_ip?: string;
  ingress_mode?: IngressMode;
  ingress_placement?: { hosts?: string[]; count?: number };
  enable_nfsv3?: boolean;
  enable_rdma?: boolean;
  rdma_port?: number;
  monitoring_port?: number;
}
