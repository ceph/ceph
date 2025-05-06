export interface NvmeofGateway {
  cli_version: string;
  version: string;
  name: string;
  group: string;
  addr: string;
  port: string;
  load_balancing_group: string;
  spdk_version: string;
}

export interface NvmeofSubsystem {
  nqn: string;
  serial_number: string;
  model_number: string;
  min_cntlid: number;
  max_cntlid: number;
  namespace_count: number;
  subtype: string;
  max_namespaces: number;
}

export interface NvmeofSubsystemInitiator {
  nqn: string;
}

export interface NvmeofListener {
  host_name: string;
  trtype: string;
  traddr: string;
  adrfam: number; // 0: IPv4, 1: IPv6
  trsvcid: number; // 4420
  id?: number; // for table
  full_addr?: string; // for table
}

export interface NvmeofSubsystemNamespace {
  nsid: number;
  uuid: string;
  bdev_name: string;
  rbd_image_name: string;
  rbd_pool_name: string;
  load_balancing_group: number;
  rbd_image_size: number;
  block_size: number;
  rw_ios_per_second: number;
  rw_mbytes_per_second: number;
  r_mbytes_per_second: number;
  w_mbytes_per_second: number;
}
