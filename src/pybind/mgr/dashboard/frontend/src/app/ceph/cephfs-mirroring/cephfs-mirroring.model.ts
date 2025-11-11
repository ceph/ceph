export interface Snap {
  id: number;
  name: string;
  sync_duration: number;
  sync_time_stamp: string;
  sync_bytes: number;
}

export interface Cluster {
  state: string;
  last_synced_snap: Snap;
  snaps_synced: number;
  snaps_deleted: number;
  snaps_renamed: number;
}

export interface ClusterRow {
  cluster_name?: string;
  fs_name: string;
  client_name: string;
  rados_inst: string;
  dir_count: number;
  local_cluster_name?: string;
}

export interface MirroringRow {
  snapshot: string;
  state: string;
  last_snap: string;
  duration: string;
  last_sync_time: string;
  progress: string;
}

export interface Peer {
  remote: {
    client_name: string;
    cluster_name: string;
    fs_name: string;
  };
  local: {
    cluster_name: string;
  };
}

export interface DummyData {
  rados_inst: string;
  peers: Record<string, Peer>;
  snap_dirs: {
    dir_count: number;
  };
}
