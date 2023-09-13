export enum OsdDeploymentOptions {
  COST_CAPACITY = 'cost_capacity',
  THROUGHPUT = 'throughput_optimized',
  IOPS = 'iops_optimized'
}

export interface DeploymentOption {
  name: OsdDeploymentOptions;
  title: string;
  desc: string;
  capacity: number;
  available: boolean;
  hdd_used: number;
  used: number;
  nvme_used: number;
  ssd_used: number;
}

export interface DeploymentOptions {
  options: {
    [key in OsdDeploymentOptions]: DeploymentOption;
  };
  recommended_option: OsdDeploymentOptions;
}
