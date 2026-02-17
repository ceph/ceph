export interface MountData {
  clusterFSID: string;
  fsName: string;
  path: string;
}

export const PERMISSION_NAMES = {
  READ: 'read',
  WRITE: 'write',
  SNAPSHOT: 'snapshot',
  QUOTA: 'quota',
  ROOTSQUASH: 'rootSquash'
} as const;

export interface RemoteInfo {
  client_name: string;
  cluster_name: string;
  fs_name: string;
}

export interface PeerStats {
  failure_count: number;
  recovery_count: number;
}

export interface Peer {
  uuid: string;
  remote: RemoteInfo;
  stats: PeerStats;
}

export interface Filesystem {
  filesystem_id: number;
  name: string;
  directory_count: number;
  peers: Peer[];
  id: string;
}

export interface Daemon {
  daemon_id: number;
  filesystems: Filesystem[];
}

export interface MirroringRow {
  remote_cluster_name: string;
  fs_name: string;
  local_fs_name?: string;
  client_name: string;
  directory_count: number;
  peerId?: string;
  id?: string;
}

export type CephfsPool = {
  pool: string;
  used: number;
};

export type CephfsDetail = {
  id: number;
  mdsmap: {
    info: Record<string, any>;
    fs_name: string;
    enabled: boolean;
    [key: string]: any;
  };
  mirror_info?: {
    peers?: Record<string, string>;
  };
  cephfs: {
    id: number;
    name: string;
    pools: CephfsPool[];
    flags?: {
      enabled?: boolean;
    };
    mirror_info?: {
      peers?: Record<string, unknown>;
    };
  };
};

export type FilesystemRow = {
  id: number;
  name: string;
  pools: string[];
  used: string;
  mdsStatus: MdsStatus;
  mirroringStatus: MirroringStatus;
};

export type MdsStatus = 'Active' | 'Warning' | 'Inactive';

export type MirroringStatus = 'Enabled' | 'Disabled';

export const MDS_STATE = {
  UP_ACTIVE: 'up:active',
  UP_STARTING: 'up:starting',
  UP_REJOIN: 'up:rejoin',
  DOWN_FAILED: 'down:failed',
  DOWN_STOPPED: 'down:stopped',
  DOWN_CRASHED: 'down:crashed',
  UNKNOWN: 'unknown'
} as const;

export const MDS_STATUS: Record<MdsStatus, MdsStatus> = {
  Active: 'Active',
  Warning: 'Warning',
  Inactive: 'Inactive'
} as const;

export const MIRRORING_STATUS: Record<MirroringStatus, MirroringStatus> = {
  Enabled: 'Enabled',
  Disabled: 'Disabled'
} as const;

const MDS_STATE_TO_STATUS: Record<string, MdsStatus> = {
  [MDS_STATE.UP_ACTIVE]: MDS_STATUS.Active,
  [MDS_STATE.UP_STARTING]: MDS_STATUS.Warning,
  [MDS_STATE.UP_REJOIN]: MDS_STATUS.Warning,
  [MDS_STATE.DOWN_FAILED]: MDS_STATUS.Inactive,
  [MDS_STATE.DOWN_STOPPED]: MDS_STATUS.Inactive,
  [MDS_STATE.DOWN_CRASHED]: MDS_STATUS.Inactive
};

export function mdsStateToStatus(state: string | undefined): MdsStatus {
  const status = state ? MDS_STATE_TO_STATUS[state] : undefined;
  return status ?? MDS_STATUS.Inactive;
}

export type DaemonResponse = Daemon[];
