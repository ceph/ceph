interface Key {
  access_key: string;
  active: boolean;
  secret_key: string;
  user: string;
}

interface SwiftKey {
  active: boolean;
  secret_key: string;
  user: string;
}

interface Cap {
  perm: string;
  type: string;
}

interface Subuser {
  id: string;
  permissions: string;
}

interface BucketQuota {
  check_on_raw: boolean;
  enabled: boolean;
  max_objects: number;
  max_size: number;
  max_size_kb: number;
}

interface UserQuota {
  check_on_raw: boolean;
  enabled: boolean;
  max_objects: number;
  max_size: number;
  max_size_kb: number;
}

interface Stats {
  num_objects: number;
  size: number;
  size_actual: number;
  size_utilized: number;
  size_kb: number;
  size_kb_actual: number;
  size_kb_utilized: number;
}

export interface RgwUser {
  account_id: string;
  admin: boolean;
  bucket_quota: BucketQuota;
  caps: Cap[];
  create_date: string;
  default_placement: string;
  default_storage_class: string;
  display_name: string;
  email: string;
  full_user_id: string;
  group_ids: any[];
  keys: Key[];
  max_buckets: number;
  mfa_ids: any[];
  op_mask: string;
  path: string;
  placement_tags: any[];
  stats: Stats;
  subusers: Subuser[];
  suspended: number;
  swift_keys: SwiftKey[];
  system: boolean;
  tags: any[];
  tenant: string;
  temp_url_keys: any[];
  type: string;
  uid: string;
  user_id: string;
  user_quota: UserQuota;
}
