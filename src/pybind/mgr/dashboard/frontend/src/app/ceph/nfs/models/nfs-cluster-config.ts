export interface NFSBackend {
  hostname: string;
  ip: string;
  port?: number;
  status?: string;
}

export interface NFSCluster {
  name: string;
  virtual_ip?: string | number | null;
  port?: number;
  backend: NFSBackend[];
  deployment_type?: string;
  enable_nfsv3?: boolean;
  enable_rdma?: boolean;
}
