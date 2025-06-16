export interface Host {
  ceph_version: string;
  services: Array<{ type: string; id: string }>;
  sources: {
    ceph: boolean;
    orchestrator: boolean;
  };
  hostname: string;
  addr: string;
  labels: string[];
  status: any;
  service_instances: Array<{ type: string; count: number }>;
}
