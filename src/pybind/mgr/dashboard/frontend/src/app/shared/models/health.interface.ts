export interface MonMap {
  monmap: {
    mons: Record<string, any>[];
  };
  quorum: number[];
}

export interface MgrMap {
  active_name: string;
  standbys: string[];
}

export interface OsdMap {
  osds: Osd[];
}

export interface PgStatus {
  object_stats: ObjectStats;
  statuses: Status;
  pgs_per_osd: number;
}

export interface MdsMap {
  filesystems: any[];
  standbys: any[];
}

export interface IscsiMap {
  up: number;
  down: number;
}

interface ObjectStats {
  num_objects: number;
  num_object_copies: number;
  num_objects_degraded: number;
  num_objects_misplaced: number;
  num_objects_unfound: number;
}

interface Status {
  'active+clean': number;
}

interface Osd {
  in: number;
  up: number;
  state: string[];
}
