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
  mdsStatus: string;
  mirroringStatus: string;
};

export type DaemonResponse = Daemon[];
