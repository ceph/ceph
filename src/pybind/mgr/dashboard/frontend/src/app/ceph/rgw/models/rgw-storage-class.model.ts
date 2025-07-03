export interface ZoneGroupDetails {
  default_zonegroup: string;
  name: string;
  zonegroups: ZoneGroup[];
}

export interface StorageClass {
  storage_class?: string;
  endpoint?: string;
  region?: string;
  placement_target: string;
  zonegroup_name?: string;
}

export interface StorageClassDetails {
  target_path: string;
  access_key: string;
  secret: string;
  multipart_min_part_size: number;
  multipart_sync_threshold: number;
  host_style: string;
  retain_head_object: boolean;
}

export interface S3Details {
  endpoint: string;
  access_key: string;
  storage_class: string;
  target_path: string;
  target_storage_class: string;
  region: string;
  secret: string;
  multipart_min_part_size: number;
  multipart_sync_threshold: number;
  host_style: boolean;
}

export interface TierTarget {
  key: string;
  val: {
    storage_class: string;
    tier_type: string;
    retain_head_object: boolean;
    allow_read_through: boolean;
    read_through_restore_days: number;
    restore_storage_class: string;
    s3?: S3Details;
  };
}

export interface Target {
  storage_classes: string[];
  name: string;
  tier_targets: TierTarget[];
}

export interface StorageClassDetails {
  target_path: string;
  access_key: string;
  secret: string;
  multipart_min_part_size: number;
  multipart_sync_threshold: number;
  host_style: string;
}

export interface ZoneGroup {
  name: string;
  id: string;
  placement_targets?: Target[];
}

export interface RequestModel {
  zone_group: string;
  placement_targets: PlacementTarget[];
}

export interface PlacementTarget {
  tags: string[];
  placement_id: string;
  tier_type?: TIER_TYPE;
  tier_config?: {
    endpoint: string;
    access_key: string;
    secret: string;
    target_path: string;
    retain_head_object: boolean;
    allow_read_through: boolean;
    region: string;
    multipart_sync_threshold: number;
    multipart_min_part_size: number;
  };
  storage_class?: string;
  name?: string;
  tier_targets?: TierTarget[];
}

export const TIER_TYPE = {
  LOCAL: 'local',
  CLOUD_TIER: 'cloud-s3'
} as const;

export const DEFAULT_PLACEMENT = 'default-placement';

export type TIER_TYPE = typeof TIER_TYPE[keyof typeof TIER_TYPE];
