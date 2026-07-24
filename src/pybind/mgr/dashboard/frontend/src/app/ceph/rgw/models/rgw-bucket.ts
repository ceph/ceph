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
  tagset?: Record<string, string | number | boolean | null | undefined>;
  acl?: string;
  lock_enabled?: boolean;
  lock_mode?: 'GOVERNANCE' | 'COMPLIANCE';
  lock_retention_period_days?: number;
  encryption?: string;
  encryption_type?: string;
  key_id?: string;
  mfa_delete?: string;
  bucket_policy?: string | Record<string, unknown>;
  replication?: {
    sync_policy_active?: boolean;
    replication_rules_configured?: boolean;
    policy?: Record<string, any>;
  };
  lifecycle?: string | Record<string, unknown>;
  lifecycle_progress?: Array<{ bucket: string; status: string; started: string }>;
  bucket_size?: number;
  size_usage?: number;
  num_objects?: number;
  object_usage?: number;
}
