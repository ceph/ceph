export interface Bucket {
  bucket: string;
  tenant: string;
  versioning: string;
  zonegroup: string;
  placement_rule: string;
  explicit_placement: {
    data_pool: string;
    data_extra_pool: string;
    index_pool: string;
  };
  id: string;
  marker: string;
  index_type: string;
  index_generation: number;
  num_shards: number;
  reshard_status: string;
  judge_reshard_lock_time: string;
  object_lock_enabled: boolean;
  mfa_enabled: boolean;
  owner: string;
  ver: string;
  master_ver: string;
  mtime: string;
  creation_time: string;
  max_marker: string;
  usage: Record<string, any>;
  bucket_quota: {
    enabled: boolean;
    check_on_raw: boolean;
    max_size: number;
    max_size_kb: number;
    max_objects: number;
  };
  read_tracker: number;
  bid: string;
}
