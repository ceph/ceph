export interface NFSBackend {
  hostname: string;
  ip: string;
  port?: number;
  status?: string;
}

export interface NFSCluster {
  name: string;
  virtual_ip: string | null;
  port?: number;
  backend: NFSBackend[];
  placement: Record<string, unknown>;
  enable_nfsv3?: boolean;
  enable_rdma?: boolean;
  deployment_type?: string;
  ingress_mode?: string;
  monitor_port?: number;
}

export interface NFSClusterOption {
  cluster_id: string;
  enable_rdma?: boolean;
}

export interface NFSShare {
  cluster_id?: string;
  export_id?: number;
}
