export class RgwRealm {
  id: string;
  name: string;
  current_period: string;
  epoch: number;
}

export class RgwZonegroup {
  id: string;
  name: string;
  api_name: string;
  is_master: boolean;
  endpoints: string;
  hostnames: string[];
  hostnames_s3website: string[];
  master_zone: string;
  zones: RgwZone[];
  placement_targets: any[];
  default_placement: string;
  realm_id: string;
  sync_policy: object;
  enabled_features: string[];
}

export class RgwZone {
  id: string;
  name: string;
  domain_root: string;
  control_pool: string;
  gc_pool: string;
  lc_pool: string;
  log_pool: string;
  intent_log_pool: string;
  usage_log_pool: string;
  roles_pool: string;
  reshard_pool: string;
  user_keys_pool: string;
  user_email_pool: string;
  user_swift_pool: string;
  user_uid_pool: string;
  otp_pool: string;
  system_key: SystemKey;
  placement_pools: any[];
  realm_id: string;
  notif_pool: string;
  endpoints: string;
}

export class SystemKey {
  access_key: string;
  secret_key: string;
}
