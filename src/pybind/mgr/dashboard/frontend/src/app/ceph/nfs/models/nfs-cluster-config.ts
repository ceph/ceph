export interface NFSBackend {
  hostname: string;
  ip: string;
  port?: number;
  status?: string;
}

export type NFSHostPattern = string | { pattern: string; pattern_type?: string };

export interface NFSClusterPlacement {
  hosts?: string[];
  count?: number;
  label?: string;
  host_pattern?: NFSHostPattern;
}

export interface NFSCluster {
  name: string;
  virtual_ip: string | null;
  port?: number;
  monitor_port?: number;
  backend: NFSBackend[];
  deployment_type?: string;
  ingress_mode?: string;
  placement?: NFSClusterPlacement;
  enable_rdma?: boolean;
  enable_nfsv3?: boolean;
}

export interface NFSClusterOption {
  cluster_id: string;
  enable_rdma?: boolean;
}

export type NFSClusterStatus = 'Active' | 'Degraded' | 'Stopped' | 'Unknown';

/**
 * Derives the cluster status from its backend daemons.
 * - Unknown  : no backend daemons reported
 * - Active   : every daemon is in 'running' state
 * - Degraded : at least one (but not all) daemon is running
 * - Stopped  : no daemon is running
 */
export function getClusterStatus(backends: NFSBackend[] = []): NFSClusterStatus {
  if (!backends.length) {
    return 'Unknown';
  }
  const running = backends.filter((b) => b.status?.toLowerCase() === 'running').length;
  if (running === backends.length) {
    return 'Active';
  }
  if (running > 0) {
    return 'Degraded';
  }
  return 'Stopped';
}
