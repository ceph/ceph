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
  zone_name?: string;
  data_pool?: string;
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
    's3-glacier'?: S3Glacier;
  };
}

export interface Target {
  name: string;
  tier_targets: TierTarget[];
  storage_classes?: string[];
}

export interface StorageClassDetails {
  tier_type: string;
  target_path: string;
  access_key: string;
  secret: string;
  multipart_min_part_size: number;
  multipart_sync_threshold: number;
  host_style: string;
  allow_read_through: boolean;
  storage_class: string;
  zonegroup_name?: string;
  placement_target?: string;
  glacier_restore_days?: number;
  glacier_restore_tier_type?: string;
  read_through_restore_days?: number;
  restore_storage_class?: string;
  retain_head_object?: boolean;
  acls?: ACL[];
  acl_mappings?: ACL[];
  zone_name?: string;
  data_pool?: string;
}

export interface ZoneGroup {
  name: string;
  id: string;
  placement_targets?: Target[];
  zones?: string[];
}

export interface ZoneRequest {
  zone_name: string;
  placement_target: string;
  storage_class: string;
  data_pool: string;
}
export interface StorageClassPool {
  data_pool: string;
}

export interface PlacementPool {
  key: string;
  val: {
    storage_classes: {
      [storage_class: string]: StorageClassPool;
    };
  };
}

export interface Zone {
  name: string;
  placement_pools: PlacementPool[];
}

export interface AllZonesResponse {
  zones: Zone[];
}

export interface ACL {
  key: string;
  val: ACLVal;
}

export interface ACLVal extends AclMapping {
  type: string;
}

export interface AclMapping {
  source_id: string;
  dest_id: string;
}

export interface GroupedACLs {
  [type: string]: AclMapping[];
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
  acl_mappings?: ACL[];
}
export interface S3Glacier {
  glacier_restore_days: number;
  glacier_restore_tier_type: string;
}

export interface RequestModel {
  zone_group: string;
  placement_targets: PlacementTarget[];
}

export interface PlacementTarget {
  placement_id?: string;
  tags?: string[];
  tier_type?: TIER_TYPE;
  tier_config_rm?: TierConfigRm;
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
    glacier_restore_days?: number;
    glacier_restore_tier_type?: string;
    restore_storage_class?: string;
    read_through_restore_days?: number;
    target_storage_class?: string;
    acls?: ACL[];
  };
  storage_class?: string;
  name?: string;
  tier_targets?: TierTarget[];
  data_pool?: string;
  placement_target?: string;
}

export interface TierConfigRm {
  [key: string]: string;
}

export interface TypeOption {
  value: string;
  label: string;
}

export interface TextLabels {
  targetPathText: string;
  targetEndpointText: string;
  targetRegionText: string;
  multipartMinPartText: string;
  storageClassText: string;
  multipartSyncThresholdText: string;
  targetSecretKeyText: string;
  targetAccessKeyText: string;
  retainHeadObjectText: string;
  allowReadThroughText: string;
  glacierRestoreDayText: string;
  glacierRestoreTiertypeText: string;
  tiertypeText: string;
  restoreDaysText: string;
  readthroughrestoreDaysText: string;
  restoreStorageClassText: string;
}

export const CLOUD_TIER_REQUIRED_FIELDS = [
  'region',
  'target_endpoint',
  'access_key',
  'secret_key',
  'target_path'
];

export const GLACIER_REQUIRED_FIELDS = [
  'region',
  'target_endpoint',
  'access_key',
  'secret_key',
  'target_path',
  'glacier_restore_tier_type',
  'restore_storage_class'
];

export const TIER_TYPE = {
  LOCAL: 'local',
  CLOUD_TIER: 'cloud-s3',
  GLACIER: 'cloud-s3-glacier'
} as const;

export const STORAGE_CLASS_CONSTANTS = {
  DEFAULT_GLACIER_RESTORE_DAYS: 1,
  DEFAULT_READTHROUGH_RESTORE_DAYS: 1,
  DEFAULT_MULTIPART_SYNC_THRESHOLD: 33554432,
  DEFAULT_MULTIPART_MIN_PART_SIZE: 33554432,
  DEFAULT_STORAGE_CLASS: 'Standard'
} as const;

export const DEFAULT_PLACEMENT = 'default-placement';

export type TIER_TYPE = typeof TIER_TYPE[keyof typeof TIER_TYPE];

export const TIER_TYPE_DISPLAY = {
  LOCAL: 'Local',
  CLOUD_TIER: 'Cloud S3',
  GLACIER: 'Cloud S3 Glacier'
};

export const GLACIER_TARGET_STORAGE_CLASS = $localize`GLACIER`;

export const ALLOW_READ_THROUGH_TEXT = $localize`Enables fetching objects from remote cloud S3 if not found locally.`;

export const MULTIPART_MIN_PART_TEXT = $localize`It specifies that objects this size or larger are transitioned to the cloud using multipart upload.`;

export const MULTIPART_SYNC_THRESHOLD_TEXT = $localize`It specifies the minimum part size to use when transitioning objects using multipart upload.`;

export const TARGET_PATH_TEXT = $localize`Target Path refers to the storage location (e.g., bucket or container) in the cloud where data will be stored.`;

export const TARGET_REGION_TEXT = $localize`The region of the remote cloud service where storage is located.`;

export const TARGET_ENDPOINT_TEXT = $localize`The URL endpoint of the remote cloud service for accessing storage.`;

export const TARGET_ACCESS_KEY_TEXT = $localize`To view or copy your access key, go to your cloud service's user management or credentials section, find your user profile, and locate the access key. You can view and copy the key by following the instructions provided.`;

export const TARGET_SECRET_KEY_TEXT = $localize`To view or copy your secret key, go to your cloud service's user management or credentials section, find your user profile, and locate the secret key. You can view and copy the key by following the instructions provided.`;

export const RETAIN_HEAD_OBJECT_TEXT = $localize`Retain object metadata after transition to the cloud.`;

export const HOST_STYLE = $localize`The URL format for accessing the remote S3 endpoint:
  - 'Path': Use for a path-based URL
  - 'Virtual': Use for a domain-based URL`;

export const LOCAL_STORAGE_CLASS_TEXT = $localize`Local storage uses on-premises or directly attached devices for data storage.`;

export const CLOUDS3_STORAGE_CLASS_TEXT = $localize`Cloud S3 storage uses Amazon S3-compatible cloud services for tiering.`;

export const GLACIER_STORAGE_CLASS_TEXT = $localize`Glacier storage uses Amazon S3 Glacier for low-cost, long-term archival data storage.`;

export const GLACIER_RESTORE_DAY_TEXT = $localize`Refers to number of days to the object will be restored on glacier/tape endpoint.`;

export const GLACIER_RESTORE_TIER_TYPE_TEXT = $localize`Restore retrieval type.`;

export const STANDARD_TIER_TYPE_TEXT = $localize`Standard glacier restore tier type restores data in 3–5 hours.`;

export const EXPEDITED_TIER_TYPE_TEXT = $localize`Expedited glacier restore tier type restores in 1–5 minutes (faster but costlier).`;

export const RESTORE_DAYS_TEXT = $localize`Refers to number of days to the object will be restored on glacier/tape endpoint.`;

export const READTHROUGH_RESTORE_DAYS_TEXT = $localize`The days for which objects restored via read-through are retained.`;

export const RESTORE_STORAGE_CLASS_TEXT = $localize`The storage class to which object data is to be restored.`;

export const ZONEGROUP_TEXT = $localize`A Zone Group is a logical grouping of one or more zones that share the same data
                  and metadata, allowing for multi-site replication and geographic distribution of
                  data.`;

export type AclType = 'id' | 'email' | 'uri';

export interface AclLabelAndHelper {
  source: string;
  destination: string;
}

export interface AclMaps {
  [key: string]: AclLabelAndHelper & {
    [field: string]: string;
  };
}

export enum AclLabel {
  source = 'Source',
  destination = 'Destination'
}

export enum AclFieldType {
  Source = 'source',
  Destination = 'destination'
}

export const AclTypeOptions = [
  { value: 'id', label: 'ID' },
  { value: 'email', label: 'Email' },
  { value: 'uri', label: 'URI' }
] as const;

export const AclTypeConst = {
  ID: 'id',
  EMAIL: 'email',
  URI: 'uri'
} as const;

export const AclTypeLabel: AclMaps = {
  id: {
    source: $localize`Source User`,
    destination: $localize`Destination User`
  },
  email: {
    source: $localize`Source Email`,
    destination: $localize`Destination Email`
  },
  uri: {
    source: $localize`Source URI`,
    destination: $localize`Destination URI`
  }
};

export const AclHelperText: AclMaps = {
  id: {
    source: $localize`The unique user ID in the source system.`,
    destination: $localize`The unique user ID in the destination system.`
  },
  email: {
    source: $localize`The email address of the source user.`,
    destination: $localize`The email address of the destination user.`
  },
  uri: {
    source: $localize`The URI identifying the source group or user.`,
    destination: $localize`The URI identifying the destination group or user.`
  }
};

export const POOL = {
  PATH: '/pool/create'
};
