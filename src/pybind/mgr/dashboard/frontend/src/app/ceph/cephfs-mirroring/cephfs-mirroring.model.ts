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
  client_name: string;
  directory_count: number;
  peerId?: string;
  id?: string;
}

export type DaemonResponse = Daemon[];
