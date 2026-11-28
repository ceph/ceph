export interface MultiCluster {
  name: string;
  url: string;
  user: string;
  // Only returned for the cluster that has just been selected (see `set_config`)
  token?: string;
  cluster_alias: string;
  cluster_connection_status: number;
  ssl_verify: boolean;
  ssl_certificate: string;
  ttl: number;
}

export interface MultiClusterConfig {
  current_url: string;
  current_user: string;
  hub_url: string;
  config: {
    [clusterId: string]: MultiCluster[];
  };
}
