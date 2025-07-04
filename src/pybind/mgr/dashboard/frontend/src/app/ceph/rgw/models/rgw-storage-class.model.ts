export interface ZoneGroupDetails {
  default_zonegroup: string;
  name: string;
  zonegroups: ZoneGroup[];
}

export interface StorageClass {
  placement_target: string;
  storage_class?: string;
  endpoint?: string;
  region?: string;
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
  allow_read_through: boolean;
}

export interface TierTarget {
  key: string;
  val: {
    storage_class: string;
    tier_type: string;
    retain_head_object: boolean;
    allow_read_through: boolean;
    s3?: S3Details;
  };
}

export interface Target {
  name: string;
  tier_targets: TierTarget[];
  storage_classes?: string[];
}

export interface StorageClassDetails {
  target_path: string;
  access_key: string;
  secret: string;
  multipart_min_part_size: number;
  multipart_sync_threshold: number;
  host_style: string;
  zonegroup_name?: string;
  placement_targets?: string;
}

export interface ZoneGroup {
  name: string;
  id: string;
  placement_targets?: Target[];
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
  retain_head_object?: boolean;
  allow_read_through?: boolean;
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
  CLOUD_TIER: 'cloud-s3',
  GLACIER: 'cloud-s3-glacier'
} as const;

export const DEFAULT_PLACEMENT = 'default-placement';

export const ALLOW_READ_THROUGH_TEXT =
  'Enables fetching objects from remote cloud S3 if not found locally.';

export const MULTIPART_MIN_PART_TEXT =
  'It specifies that objects this size or larger are transitioned to the cloud using multipart upload.';

export const MULTIPART_SYNC_THRESHOLD_TEXT =
  'It specifies the minimum part size to use when transitioning objects using multipart upload.';

export const TARGET_PATH_TEXT =
  'Target Path refers to the storage location (e.g., bucket or container) in the cloud where data will be stored.';

export const TARGET_REGION_TEXT =
  'The region of the remote cloud service where storage is located.';

export const TARGET_ENDPOINT_TEXT =
  'The URL endpoint of the remote cloud service for accessing storage.';

export const TARGET_ACCESS_KEY_TEXT =
  "To view or copy your access key, go to your cloud service's user management or credentials section, find your user profile, and locate the access key. You can view and copy the key by following the instructions provided.";

export const TARGET_SECRET_KEY_TEXT =
  "To view or copy your secret key, go to your cloud service's user management or credentials section, find your user profile, and locate the secret key. You can view and copy the key by following the instructions provided.";

export const RETAIN_HEAD_OBJECT_TEXT = 'Retain object metadata after transition to the cloud.';

export const HOST_STYLE = `The URL format for accessing the remote S3 endpoint:
  - 'Path': Use for a path-based URL
  - 'Virtual': Use for a domain-based URL`;

export const LOCAL_STORAGE_CLASS_TEXT = $localize`Local storage uses on-premises or directly attached devices for data storage.`;

export const CLOUDS3_STORAGE_CLASS_TEXT = $localize`Cloud S3 storage uses Amazon S3-compatible cloud services for tiering.`;

export type TIER_TYPE = typeof TIER_TYPE[keyof typeof TIER_TYPE];

export const TIER_TYPE_DISPLAY = {
  LOCAL: 'Local',
  CLOUD_TIER: 'Cloud S3',
  GLACIER: 'Cloud S3 Glacier'
};
