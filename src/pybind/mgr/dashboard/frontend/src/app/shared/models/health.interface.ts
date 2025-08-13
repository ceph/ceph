export interface IscsiMap {
  up: number;
  down: number;
}

export interface HealthCheck {
  severity: string;
  summary: {
    message: string;
    count: number;
  };
  muted: boolean;
}

export interface Health {
  status: string;
  checks: Record<string, HealthCheck>;
  mutes: string[];
}

export interface MonMap {
  num_mons: number;
}

export interface OsdMap {
  in: number;
  up: number;
  num_osds: number;
}

export interface PgStateCount {
  state_name: string;
  count: number;
}
export interface PgMap {
  pgs_by_state: PgStateCount[];
  num_pools: number;
  bytes_used: number;
  bytes_total: number;
  num_pgs: number;
}

export interface HealthMapCommon {
  num_standbys: number;
  num_active: number;
}

export interface HealthSnapshotMap {
  fsid: string;
  health: Health;
  monmap: MonMap;
  osdmap: OsdMap;
  pgmap: PgMap;
  mgrmap: HealthMapCommon;
  fsmap: HealthMapCommon;
  num_rgw_gateways: number;
  num_iscsi_gateways: { up: number; down: number };
  num_hosts: number;
}
