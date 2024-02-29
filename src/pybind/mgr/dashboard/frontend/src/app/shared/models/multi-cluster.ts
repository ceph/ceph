export interface MultiCluster {
  name: string;
  url: string;
  user: string;
  token: string;
  cluster_alias: string;
  cluster_connection_status: number;
  ssl_verify: boolean;
  ssl_certificate: string;
  ttl: number;
}
