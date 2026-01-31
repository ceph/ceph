export interface CephCaps {
  mds?: string;
  mgr?: string;
  mon?: string;
  osd?: string;
}

export interface CephUser {
  entity: string;
  key: string;
  caps?: CephCaps;
}

export interface MonitorData {
  in_quorum: { public_addr: string }[];
}
