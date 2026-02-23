import { CephServiceSpec } from './service.interface';

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
  allow_any_host?: boolean;
  enable_ha?: boolean;
  gw_group?: string;
  initiator_count?: number;
  has_dhchap_key: boolean;
}

export interface NvmeofSubsystemData extends NvmeofSubsystem {
  auth?: string;
  hosts?: number;
}

export interface NvmeofSubsystemInitiator {
  nqn: string;
  use_dhchap?: string;
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
  rbd_image_size: number | string;
  block_size: number;
  rw_ios_per_second: number | string;
  rw_mbytes_per_second: number | string;
  r_mbytes_per_second: number | string;
  w_mbytes_per_second: number | string;
  ns_subsystem_nqn?: string; // Field from JSON
  subsystem_nqn?: string; // Keep for compatibility if needed, but JSON has ns_subsystem_nqn
}

export interface NvmeofGatewayGroup extends CephServiceSpec {
  name: string;
  gatewayCount: {
    running: number;
    error: number;
  };
  subSystemCount: number;
  nodeCount: number;
}

export enum AUTHENTICATION {
  Unidirectional = 'unidirectional',
  Bidirectional = 'bidirectional'
}

export const HOST_TYPE = {
  ALL: 'all',
  SPECIFIC: 'specific'
};

/**
 * Determines the authentication status of a subsystem based on PSK and initiators.
 * Can be reused across subsystem pages.
 */
export function getSubsystemAuthStatus(
  subsystem: NvmeofSubsystem,
  _initiators: NvmeofSubsystemInitiator[] | { hosts?: NvmeofSubsystemInitiator[] }
): string {
  // Import enum value strings to avoid circular dependency
  const NO_AUTH = 'No authentication';
  const UNIDIRECTIONAL = 'Unidirectional';
  const BIDIRECTIONAL = 'Bi-directional';

  let hostsList: NvmeofSubsystemInitiator[] = [];
  if (_initiators && 'hosts' in _initiators && Array.isArray(_initiators.hosts)) {
    hostsList = _initiators.hosts;
  } else if (Array.isArray(_initiators)) {
    hostsList = _initiators as NvmeofSubsystemInitiator[];
  }

  let auth = NO_AUTH;

  const hostHasDhchapKey = hostsList.some((host) => !!host.use_dhchap);

  if (hostHasDhchapKey) {
    auth = UNIDIRECTIONAL;
  }

  if (subsystem.has_dhchap_key && hostHasDhchapKey) {
    auth = BIDIRECTIONAL;
  }

  return auth;
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
