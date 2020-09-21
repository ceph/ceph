import { RbdConfigurationEntry } from '../models/configuration';

export interface RbdPool {
  pool_name: string;
  status: number;
  value: RbdImage[];
}

export interface RbdImage {
  disk_usage: number;
  stripe_unit: number;
  name: string;
  parent: any;
  pool_name: string;
  num_objs: number;
  block_name_prefix: string;
  snapshots: any[];
  obj_size: number;
  data_pool: string;
  total_disk_usage: number;
  features: number;
  configuration: RbdConfigurationEntry[];
  timestamp: string;
  id: string;
  features_name: string[];
  stripe_count: number;
  order: number;
  size: number;
}
