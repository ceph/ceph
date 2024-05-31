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
