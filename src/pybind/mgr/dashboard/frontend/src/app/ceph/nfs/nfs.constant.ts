export interface ComboBoxContent {
  content: string;
  selected?: boolean;
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
  ingress_mode: IngressMode;
  ingress_placement_mode: string;
  protocol_version: string;
  enable_rdma: boolean;
  rdma_port: number;
  monitoring_port: number;
}

export enum Placement {
  Auto = 'auto',
  Host = 'hosts',
  IngressHost = 'ingress_hosts',
  Custom = 'custom'
}

export enum CustomPlacementType {
  Label = 'label',
  HostPattern = 'host_pattern'
}

export enum IngressMode {
  HaproxyProtocol = 'haproxy-protocol',
  HaproxyStandard = 'haproxy-standard',
  KeepaliveOnly = 'keepalive-only'
}
