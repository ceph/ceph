export interface Host {
  addr: string;
  ceph_version: string;
  hostname: string;
  labels: string[];
  services: string[];
  sources: {
    ceph: boolean;
    orchestrator: boolean;
  };
  status: string;
}

export type HostList = Host[];
