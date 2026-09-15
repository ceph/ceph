export interface NFSBackend {
  hostname: string;
  ip: string;
  port: number;
  status?: string;
}

export interface NFSCluster {
  name: string;
  virtual_ip: string | null;
  port?: number;
  backend: NFSBackend[];
  enable_rdma?: boolean;
  placement?: Record<string, unknown>;
  deployment_type?: string;
  ingress_mode?: string;
  monitor_port?: number;
}

export interface NFSClusterOption {
  cluster_id: string;
  enable_rdma?: boolean;
}
