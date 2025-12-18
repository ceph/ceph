export interface ComboBoxContent {
  content: string;
}

export interface BindAddrValue {
  hostname: ComboBoxContent | string;
  ip: string;
}

export interface NfsClusterFormValue {
  cluster_id: string;
  placement: string;
  count: number;
  custom_placement_type: string;
  label: ComboBoxContent | string | null;
  host_pattern: string;
  port: number;
  networks: string[];
  bind_addrs: BindAddrValue[];
  ingress: boolean;
  virtual_ip: string;
  ingress_mode: string;
  ingress_placement_mode: string;
  protocol_version: string;
  enable_rdma: boolean;
  rdma_port: number;
  monitoring_port: number;
}
