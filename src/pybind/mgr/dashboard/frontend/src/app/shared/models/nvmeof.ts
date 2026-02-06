export interface NvmeofGateway {
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

// Form control names for NvmeofNamespacesFormComponent
export enum NsFormField {
  POOL = 'pool',
  SUBSYSTEM = 'subsystem',
  IMAGE_SIZE = 'image_size',
  NS_COUNT = 'nsCount',
  RBD_IMAGE_CREATION = 'rbd_image_creation',
  RBD_IMAGE_NAME = 'rbd_image_name',
  NAMESPACE_SIZE = 'namespace_size',
  HOST_ACCESS = 'host_access',
  INITIATORS = 'initiators'
}

export enum RbdImageCreation {
  GATEWAY_PROVISIONED = 'gateway_provisioned',
  EXTERNALLY_MANAGED = 'externally_managed'
}

export type NvmeofNamespaceListResponse =
  | NvmeofSubsystemNamespace[]
  | { namespaces: NvmeofSubsystemNamespace[] };

export type NvmeofInitiatorCandidate = {
  content: string;
  selected: boolean;
};
