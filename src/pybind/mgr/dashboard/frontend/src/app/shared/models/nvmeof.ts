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
  psk?: string;
}

export interface NvmeofSubsystemData extends NvmeofSubsystem {
  auth?: string;
  hosts?: number;
}

export interface NvmeofSubsystemInitiator {
  nqn: string;
  dhchap_key?: string;
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

  if (subsystem.psk) {
    return BIDIRECTIONAL;
  }

  let hostsList: NvmeofSubsystemInitiator[] = [];
  if (_initiators && 'hosts' in _initiators && Array.isArray(_initiators.hosts)) {
    hostsList = _initiators.hosts;
  } else if (Array.isArray(_initiators)) {
    hostsList = _initiators as NvmeofSubsystemInitiator[];
  }

  const hasDhchapKey = hostsList.some((host) => !!host.dhchap_key);
  if (hasDhchapKey) {
    return UNIDIRECTIONAL;
  }

  return NO_AUTH;
}
