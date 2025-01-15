export interface Account {
  id: string;
  tenant: string;
  name: string;
  email: string;
  quota: Quota;
  bucket_quota: Quota;
  max_users: number;
  max_roles: number;
  max_groups: number;
  max_buckets: number;
  max_access_keys: number;
}

interface Quota {
  enabled: boolean;
  check_on_raw: boolean;
  max_size: number;
  max_size_kb: number;
  max_objects: number;
}
