export interface NFSBackend {
  hostname: string;
  ip: string;
  port: number;
}

export interface NFSCluster {
  name: string;
  virtual_ip: number;
  port: number;
  backend: NFSBackend[];
}
