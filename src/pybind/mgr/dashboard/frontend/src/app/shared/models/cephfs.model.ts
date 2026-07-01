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
  fsid?: string;
  mon_host?: string;
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
  filesystem_id?: number;
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

export interface MirrorPeerListEntry {
  client_name: string;
  site_name: string;
  fs_name: string;
}

export type MirrorPeerList = Record<string, MirrorPeerListEntry>;

export interface MirrorSyncCrawl {
  state?: string;
  duration?: string;
}

export interface MirrorSyncBytes {
  sync_bytes?: string;
  total_bytes?: string;
  sync_percent?: string;
}

export interface MirrorSyncFiles {
  sync_files?: number;
  total_files?: number;
  sync_percent?: string;
}

export interface MirrorSyncedSnap {
  id?: number;
  name?: string;
  'sync-mode'?: string;
  sync_bytes?: number | string;
  sync_duration?: number | string;
  sync_time_stamp?: number | string;
  avg_read_throughput_bytes?: string;
  avg_write_throughput_bytes?: string;
  crawl?: MirrorSyncCrawl;
  datasync_queue_wait?: MirrorSyncCrawl;
  bytes?: MirrorSyncBytes;
  files?: MirrorSyncFiles;
  eta?: string;
}

export interface MirrorSyncProgress {
  state?: string;
  duration?: string;
}

export interface MirrorSyncSnapBytes {
  sync_bytes?: string;
  total_bytes?: string;
  sync_percent?: string;
}

export interface MirrorSyncSnapFiles {
  sync_files?: number;
  total_files?: number;
  sync_percent?: string;
}

export interface MirrorCurrentSyncingSnap {
  id?: number;
  name: string;
  'sync-mode'?: string;
  avg_read_throughput_bytes?: string;
  avg_write_throughput_bytes?: string;
  crawl?: MirrorSyncProgress;
  datasync_queue_wait?: MirrorSyncProgress;
  bytes?: MirrorSyncSnapBytes;
  files?: MirrorSyncSnapFiles;
  eta?: string;
}

export interface MirrorLastSyncedSnap {
  id?: number;
  name?: string;
  crawl_duration?: string;
  datasync_queue_wait_duration?: string;
  sync_duration?: string;
  sync_time_stamp?: number | string;
  sync_bytes?: number | string;
  sync_files?: number;
}

export interface MirrorDirStatus {
  state?: string;
  last_synced_snap?: MirrorLastSyncedSnap;
  current_syncing_snap?: MirrorCurrentSyncingSnap;
  current_sync_snap?: MirrorCurrentSyncingSnap;
  snaps_synced?: number;
  snaps_deleted?: number;
  snaps_renamed?: number;
  metrics_updated_at?: number | string;
}

export interface MirrorDirMetrics {
  peer?: Record<string, MirrorDirStatus>;
}

export type MirrorStatusMetrics = Record<string, MirrorDirMetrics>;

export interface MirrorStatusResponse {
  metrics?: MirrorStatusMetrics;
}

export interface DaemonOverviewInfo {
  mirrorPaths: number;
  failures: number;
  clusterName: string;
  destinationFsName: string;
  fsid: string;
  monitorEndpoint: string;
  peerUuid?: string;
}

export interface MirroringFsOverviewStats {
  mirrorPaths: number;
  syncingPaths: number;
  failures: number;
}

export interface MirroringFsDestinationCluster {
  clusterName: string;
  siteName: string;
  destinationFsName: string;
  fsid: string;
  monitorEndpoint: string;
}

export interface MirroringFsSyncInfo {
  bytesSynced: string;
  path: string;
  snapName: string;
  syncedAt: number | null;
}

export interface MirroringFsOverviewData {
  fsName: string;
  stats: MirroringFsOverviewStats;
  destination: MirroringFsDestinationCluster;
  sync: MirroringFsSyncInfo;
}

export type MirroringEntityRow = {
  entity: string;
  mdsCaps: string;
  monCaps: string;
  osdCaps: string;
};

export interface BootstrapTokenResponse {
  token?: string;
  data?: string;
}

export const CLIENT_PREFIX = 'client.';
export const MAX_TYPEAHEAD_SUGGESTIONS = 10;
export const VALID_USERNAME_PATTERN = /[^a-zA-Z0-9_-]/;

export type CephfsMirroringSetupEvent = {
  filesystem: string;
};
