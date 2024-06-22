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
