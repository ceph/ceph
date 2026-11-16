import { ExecutingTask } from '~/app/shared/models/executing-task';
import { PoolStats } from './pool-stat';

export enum PoolType {
  ERASURE = 'erasure',
  REPLICATED = 'replicated'
}

export const POOL_APPLICATION_LABELS: Record<string, string> = {
  cephfs: $localize`File system`,
  rbd: $localize`Block`,
  rgw: $localize`Object`
};

export const mapPoolApplications = (applications: string[] = []): string[] =>
  applications.map((application: string) => POOL_APPLICATION_LABELS[application] || application);

export class Pool {
  cache_target_full_ratio_micro: number;
  fast_read: boolean;
  stripe_width: number;
  flags_names: string;
  tier_of: number;
  hit_set_grade_decay_rate: number;
  use_gmt_hitset: boolean;
  last_force_op_resend_preluminous: string;
  quota_max_bytes: number;
  erasure_code_profile: string;
  expected_num_objects: number;
  size: number;
  snap_seq: number;
  auid: number;
  cache_min_flush_age: number;
  hit_set_period: number;
  min_read_recency_for_promote: number;
  target_max_objects: number;
  pg_num: number;
  pg_num_target: number;
  pg_num_pending: number;
  pg_placement_num: number;
  pg_placement_num_target: number;
  pg_autoscale_mode: string;
  pg_status: string;
  type: string;
  pool_name: string;
  cache_min_evict_age: number;
  cache_mode: string;
  min_size: number;
  cache_target_dirty_high_ratio_micro: number;
  object_hash: number;
  application_metadata: string[];
  write_tier: number;
  cache_target_dirty_ratio_micro: number;
  pool: number;
  removed_snaps: string;
  cdExecuting?: string;
  executingTasks?: ExecutingTask[];
  crush_rule: string;
  tiers: any[];
  hit_set_params: {
    type: string;
  };
  last_force_op_resend: string;
  pool_snaps: any[];
  quota_max_objects: number;
  options: {
    compression_algorithm?: string;
    compression_max_blob_size?: number;
    compression_min_blob_size?: number;
    compression_mode?: string;
    compression_required_ratio?: number;
  };
  hit_set_count: number;
  flags: number;
  target_max_bytes: number;
  hit_set_search_last_n: number;
  last_change: string;
  min_write_recency_for_promote: number;
  read_tier: number;
  stats?: PoolStats;
  cdIsBinary?: boolean;
  configuration: { source: number; name: string; value: string }[];
  migration_src?: number;
  migration_target?: number;
  migration_root?: number;
  migrating_pgs?: string;

  constructor(name: string) {
    this.pool_name = name;
  }
}

export const getPoolDataProtection = (pool?: Pool): string => {
  if (pool?.type === PoolType.ERASURE && pool.erasure_code_profile) {
    return `EC: ${pool.erasure_code_profile}`;
  }

  if (pool?.type === PoolType.REPLICATED && pool.size != null) {
    return `replica: x${pool.size}`;
  }

  return '';
};

const CEPH_RELEASES = [
  'argonaut',
  'bobtail',
  'cuttlefish',
  'dumpling',
  'emperor',
  'firefly',
  'giant',
  'hammer',
  'infernalis',
  'jewel',
  'kraken',
  'luminous',
  'mimic',
  'nautilus',
  'octopus',
  'pacific',
  'quincy',
  'reef',
  'squid',
  'tentacle',
  'umbrella'
];

/** Oldest Ceph release (OSD release and minimum compatible client) that supports pool migration. */
export const MIN_POOL_MIGRATION_RELEASE = 'umbrella';

export const isReleaseAtLeast = (current: string, minimum: string): boolean => {
  if (!current) {
    return false;
  }
  const currentIdx = CEPH_RELEASES.indexOf(current.toLowerCase());
  const minIdx = CEPH_RELEASES.indexOf(minimum.toLowerCase());
  if (minIdx < 0) {
    return false;
  }
  // Unknown release names are assumed newer than the hardcoded list (forward compatible).
  if (currentIdx < 0) {
    return true;
  }
  return currentIdx >= minIdx;
};

export const EXPERIMENTAL_FEATURES_CONFIG =
  'enable_experimental_unrecoverable_data_corrupting_features';
export const POOL_MIGRATION_FEATURE = 'poolmigration';

// Same separators as Ceph's for_each_substr(..., ";,= \t", ...) in src/common/ceph_context.cc
const parseFeatures = (value: unknown): string[] =>
  typeof value === 'string' ? value.split(/[;,= \t]+/).filter(Boolean) : [];

export const isPoolMigrationFlagEnabled = (value: unknown): boolean => {
  const features = parseFeatures(value);
  return features.includes('*') || features.includes(POOL_MIGRATION_FEATURE);
};

/** Returns the new config value with the pool migration feature added. */
export const addPoolMigrationFlag = (value: unknown): string =>
  isPoolMigrationFlagEnabled(value)
    ? (value as string)
    : [...parseFeatures(value), POOL_MIGRATION_FEATURE].join(',');

export enum PoolBusyReason {
  MIGRATING = 'migrating',
  /** ``pg_num`` is converging to ``pg_num_target``. */
  PG_NUM_CHANGE = 'pgNumChange',
  /** ``pgp_num`` (placement) is converging to ``pgp_num_target``. */
  PGP_NUM_CHANGE = 'pgpNumChange',
  /** Both ``pg_num`` and ``pgp_num`` are converging. */
  PG_CHANGE = 'pgChange',
  TASK = 'task'
}

export enum PoolAction {
  MIGRATE = 'migrate',
  EDIT = 'edit',
  DELETE = 'delete'
}

export const getActiveMigrationSourceIds = (pools: Pool[] | null | undefined): Set<number> => {
  const ids = new Set<number>();
  for (const pool of pools || []) {
    if (pool.migration_src != null) {
      ids.add(Number(pool.migration_src));
    }
  }
  return ids;
};

/**
 * Whether this pool is part of a migration that is still moving data.
 * Pass ``activeSourceIds`` from {@link getActiveMigrationSourceIds} so only the
 * stub that is the source of the current migration is included — not unrelated
 * historical ``.migrate-*`` pools.
 */
export const isPoolInActiveMigration = (pool: Pool, activeSourceIds?: Set<number>): boolean => {
  if (pool.migration_src != null) {
    return true;
  }
  if (pool.migration_target == null || !activeSourceIds) {
    return false;
  }
  return activeSourceIds.has(Number(pool.pool));
};

/**
 * Returns why an operation can't be performed on a pool right now, or null when it is idle.
 * Specific causes are checked before a generic running task, because an ongoing
 * migration is also reported as an executing task by the backend.
 *
 * @param activeSourceIds result of {@link getActiveMigrationSourceIds} when known
 */
export const getPoolBusyReason = (
  pool: Pool,
  activeSourceIds?: Set<number>
): PoolBusyReason | null => {
  if (isPoolInActiveMigration(pool, activeSourceIds)) {
    return PoolBusyReason.MIGRATING;
  }
  const pgNumChanging = pool.pg_num !== pool.pg_num_target;
  const pgpNumChanging = pool.pg_placement_num !== pool.pg_placement_num_target;
  if (pgNumChanging && pgpNumChanging) {
    return PoolBusyReason.PG_CHANGE;
  }
  if (pgNumChanging) {
    return PoolBusyReason.PG_NUM_CHANGE;
  }
  if (pgpNumChanging) {
    return PoolBusyReason.PGP_NUM_CHANGE;
  }
  return pool.cdExecuting ? PoolBusyReason.TASK : null;
};

export const transformPgStatus = (pgStatus: any): string => {
  if (!pgStatus || typeof pgStatus === 'string') {
    return pgStatus || '';
  }

  return (
    Object.entries(pgStatus)
      .map(([state, count]) => `${count} ${state}`)
      .join(', ') || ''
  );
};
