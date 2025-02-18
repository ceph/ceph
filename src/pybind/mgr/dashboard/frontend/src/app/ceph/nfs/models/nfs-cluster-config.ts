export interface NFSClusterTableModel {
  name: string;
  host: string[];
  ip_port: string[];
  virtual_ip_Port: string;
}

export interface Backend {
  hostname: string;
  ip: string;
  port: number;
}

export interface Entity {
  virtual_ip: number;
  port: number;
  backend: Backend[];
}

export interface NFSClusterListConfig {
  [name: string]: Entity;
}
