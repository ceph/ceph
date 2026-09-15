import { StorageClassQuota } from '../models/rgw-user';

export interface StorageClassUsage {
  name?: string;
  storage_class?: string;
  placement?: string;
  size?: number;
  size_actual?: number;
  size_utilized?: number;
  size_kb?: number;
  size_kb_actual?: number;
  size_kb_utilized?: number;
  num_objects?: number;
}

export interface StorageClassUsageRow {
  storage_class: string;
  placement: string;
  size: number;
  size_actual: number;
  num_objects: number;
}

export interface StorageClassMonitorRow {
  storage_class: string;
  placement: string;
  used_size: string | number;
  used_objects: number;
  enabled: string;
  max_size: string | number;
  max_objects: string | number;
  [key: string]: string | number;
}

export interface StorageClassEnforcementRow {
  storage_class: string;
  placement: string;
  tier: string;
  used_size: string | number;
  used_objects: number;
  used_size_bytes: number;
  max_size_bytes: number;
  max_objects_limit: number;
  size_percent: number;
  object_percent: number;
  enabled: string;
  max_size: string | number;
  max_objects: string | number;
  enforcement: string;
  writes_blocked: boolean;
  is_hot: boolean;
}

export const HOT_STORAGE_CLASSES = ['STANDARD', 'STANDARD_IA'];

export type StorageClassUsagePayload =
  | StorageClassUsage[]
  | Record<string, StorageClassUsage>
  | undefined
  | null;

export type StorageClassQuotaPayload =
  | StorageClassQuota[]
  | Record<string, StorageClassQuota>
  | undefined
  | null;

// Dummy usage until RGW returns per-class stats. STANDARD (hot) is over quota so
// the dashboard can show write-blocking; HDD (cold) still has headroom.
export const DUMMY_USER_STORAGE_CLASS_STATS: Record<string, StorageClassUsage> = {
  'default-placement::STANDARD': {
    size: 55834574848, // 52 GiB
    size_actual: 55834574848,
    size_utilized: 55834574848,
    size_kb: 54525952,
    size_kb_actual: 54525952,
    size_kb_utilized: 54525952,
    num_objects: 12000
  },
  'default-placement::HDD': {
    size: 42949672960, // 40 GiB
    size_actual: 42949672960,
    size_utilized: 42949672960,
    size_kb: 41943040,
    size_kb_actual: 41943040,
    size_kb_utilized: 41943040,
    num_objects: 2000
  }
};

export const DUMMY_BUCKET_STORAGE_CLASS_STATS: StorageClassUsage[] = [
  {
    name: 'STANDARD',
    size: 55834574848,
    size_actual: 55834574848,
    size_utilized: 55834574848,
    size_kb: 54525952,
    size_kb_actual: 54525952,
    size_kb_utilized: 54525952,
    num_objects: 12000
  },
  {
    name: 'HDD',
    size: 42949672960,
    size_actual: 42949672960,
    size_utilized: 42949672960,
    size_kb: 41943040,
    size_kb_actual: 41943040,
    size_kb_utilized: 41943040,
    num_objects: 2000
  }
];

// Placeholder until RGW accepts per-class quota set. STANDARD = hot, HDD = cold (#66501).
export const DUMMY_STORAGE_CLASS_QUOTAS: StorageClassQuota[] = [
  {
    storage_class: 'STANDARD',
    enabled: true,
    max_size: 53687091200, // 50 GiB
    max_objects: 10000
  },
  {
    storage_class: 'HDD',
    enabled: true,
    max_size: 214748364800, // 200 GiB
    max_objects: 50000
  }
];

export function parsePlacementClassKey(key: string): { placement: string; storage_class: string } {
  const separator = '::';
  const index = key.indexOf(separator);
  if (index === -1) {
    return { placement: '', storage_class: key };
  }
  return {
    placement: key.slice(0, index),
    storage_class: key.slice(index + separator.length)
  };
}

export function hasStorageClassUsage(payload: StorageClassUsagePayload): boolean {
  if (!payload) {
    return false;
  }
  return Array.isArray(payload) ? payload.length > 0 : Object.keys(payload).length > 0;
}

export function extractUserStorageClassStats(user?: {
  'stats.storage-classes'?: unknown;
  storage_class_stats?: unknown;
  stats?: { 'storage-classes'?: unknown };
}): StorageClassUsagePayload {
  if (!user) {
    return undefined;
  }
  return (
    (user['stats.storage-classes'] as StorageClassUsagePayload) ||
    (user.storage_class_stats as StorageClassUsagePayload) ||
    (user.stats?.['storage-classes'] as StorageClassUsagePayload)
  );
}

export function extractBucketStorageClassStats(bucket?: {
  storage_class_stats?: unknown;
  usage?: Record<string, any>;
}): StorageClassUsagePayload {
  if (!bucket) {
    return undefined;
  }
  const usageClasses = bucket.usage?.['rgw.storage-classes'] as StorageClassUsagePayload;
  return usageClasses || (bucket.storage_class_stats as StorageClassUsagePayload);
}

export function toStorageClassUsageRows(
  payload: StorageClassUsagePayload,
  fallback?: StorageClassUsagePayload
): StorageClassUsageRow[] {
  const source = hasStorageClassUsage(payload) ? payload : fallback;
  if (!hasStorageClassUsage(source) || !source) {
    return [];
  }
  if (Array.isArray(source)) {
    return source.map((item) => ({
      storage_class: item.name || item.storage_class || '',
      placement: item.placement || '',
      size: Number(item.size ?? 0),
      size_actual: Number(item.size_actual ?? item.size ?? 0),
      num_objects: Number(item.num_objects ?? 0)
    }));
  }
  return Object.entries(source).map(([key, item]) => {
    const parsed = parsePlacementClassKey(key);
    return {
      storage_class: item.name || item.storage_class || parsed.storage_class,
      placement: item.placement || parsed.placement,
      size: Number(item.size ?? 0),
      size_actual: Number(item.size_actual ?? item.size ?? 0),
      num_objects: Number(item.num_objects ?? 0)
    };
  });
}

export function normalizeStorageClassQuotas(
  quotas?: StorageClassQuotaPayload
): StorageClassQuota[] {
  if (!quotas) {
    return [];
  }
  if (Array.isArray(quotas)) {
    return quotas;
  }
  return Object.entries(quotas).map(([storageClass, quota]) => ({
    ...quota,
    storage_class: quota.storage_class || storageClass
  }));
}

export function formatStorageClassQuotaFields(
  quota: StorageClassQuota,
  formatSize?: (bytes: number) => string
): {
  enabled: string;
  max_size: string | number;
  max_objects: string | number;
} {
  const enabled = quota.enabled;
  return {
    enabled: enabled ? $localize`Yes` : $localize`No`,
    max_size: enabled
      ? quota.max_size <= -1
        ? $localize`Unlimited`
        : formatSize
          ? formatSize(quota.max_size)
          : quota.max_size
      : '-',
    max_objects: enabled
      ? quota.max_objects <= -1
        ? $localize`Unlimited`
        : quota.max_objects
      : '-'
  };
}

export function toStorageClassQuotaDisplayRows(
  quotaPayload?: StorageClassQuotaPayload,
  formatSize?: (bytes: number) => string,
  fallback: StorageClassQuota[] = DUMMY_STORAGE_CLASS_QUOTAS
): Record<string, string | number>[] {
  const quotas = normalizeStorageClassQuotas(quotaPayload);
  const list = quotas.length ? quotas : fallback;
  return list.map((quota) => ({
    storage_class: quota.storage_class,
    ...formatStorageClassQuotaFields(quota, formatSize)
  }));
}

export function toStorageClassMonitorRows(
  usagePayload: StorageClassUsagePayload,
  quotaPayload?: StorageClassQuotaPayload,
  formatSize?: (bytes: number) => string,
  usageFallback: StorageClassUsagePayload = DUMMY_USER_STORAGE_CLASS_STATS
): StorageClassMonitorRow[] {
  const usageRows = toStorageClassUsageRows(usagePayload, usageFallback);
  const quotas = normalizeStorageClassQuotas(quotaPayload);
  const quotaList = quotas.length ? quotas : DUMMY_STORAGE_CLASS_QUOTAS;
  const quotaByClass = new Map(quotaList.map((quota) => [quota.storage_class, quota]));
  const usageByClass = new Map(usageRows.map((row) => [row.storage_class, row]));
  const classNames = Array.from(
    new Set([...usageByClass.keys(), ...quotaByClass.keys()].filter(Boolean))
  );

  return classNames.map((storageClass) => {
    const usage = usageByClass.get(storageClass);
    const quota = quotaByClass.get(storageClass) || {
      storage_class: storageClass,
      enabled: false,
      max_size: -1,
      max_objects: -1
    };
    const usedSize = usage?.size_actual ?? usage?.size ?? 0;
    return {
      storage_class: storageClass,
      placement: usage?.placement || '',
      used_size: formatSize ? formatSize(usedSize) : usedSize,
      used_objects: usage?.num_objects ?? 0,
      ...formatStorageClassQuotaFields(quota, formatSize)
    };
  });
}

export function isHotStorageClass(storageClass: string): boolean {
  return HOT_STORAGE_CLASSES.includes((storageClass || '').toUpperCase());
}

export function quotaUsagePercent(used: number, limit: number): number {
  if (!(limit > 0)) {
    return 0;
  }
  return Math.round((used / limit) * 1000) / 10;
}

export function evaluateStorageClassEnforcement(
  usedSize: number,
  usedObjects: number,
  quota?: StorageClassQuota
): { writesBlocked: boolean; status: string; sizePercent: number; objectPercent: number } {
  const enabled = quota?.enabled ?? false;
  const maxSize = quota?.max_size ?? -1;
  const maxObjects = quota?.max_objects ?? -1;
  const sizePercent = quotaUsagePercent(usedSize, maxSize);
  const objectPercent = quotaUsagePercent(usedObjects, maxObjects);
  if (!enabled) {
    return {
      writesBlocked: false,
      status: $localize`Not enforced`,
      sizePercent,
      objectPercent
    };
  }
  const sizeBlocked = maxSize >= 0 && usedSize >= maxSize;
  const objectBlocked = maxObjects >= 0 && usedObjects >= maxObjects;
  const writesBlocked = sizeBlocked || objectBlocked;
  return {
    writesBlocked,
    status: writesBlocked ? $localize`Writes blocked` : $localize`Enforced`,
    sizePercent,
    objectPercent
  };
}

export function toStorageClassEnforcementRows(
  usagePayload: StorageClassUsagePayload,
  quotaPayload?: StorageClassQuotaPayload,
  formatSize?: (bytes: number) => string,
  usageFallback: StorageClassUsagePayload = DUMMY_USER_STORAGE_CLASS_STATS
): StorageClassEnforcementRow[] {
  const usageRows = toStorageClassUsageRows(usagePayload, usageFallback);
  const quotas = normalizeStorageClassQuotas(quotaPayload);
  const quotaList = quotas.length ? quotas : DUMMY_STORAGE_CLASS_QUOTAS;
  const quotaByClass = new Map(quotaList.map((quota) => [quota.storage_class, quota]));
  const usageByClass = new Map(usageRows.map((row) => [row.storage_class, row]));
  const classNames = Array.from(
    new Set([...usageByClass.keys(), ...quotaByClass.keys()].filter(Boolean))
  );

  return classNames.map((storageClass) => {
    const usage = usageByClass.get(storageClass);
    const quota = quotaByClass.get(storageClass) || {
      storage_class: storageClass,
      enabled: false,
      max_size: -1,
      max_objects: -1
    };
    const usedSize = usage?.size_actual ?? usage?.size ?? 0;
    const usedObjects = usage?.num_objects ?? 0;
    const evaluation = evaluateStorageClassEnforcement(usedSize, usedObjects, quota);
    const isHot = isHotStorageClass(storageClass);
    return {
      storage_class: storageClass,
      placement: usage?.placement || '',
      tier: isHot ? $localize`Hot` : $localize`Cold`,
      used_size: formatSize ? formatSize(usedSize) : usedSize,
      used_objects: usedObjects,
      used_size_bytes: usedSize,
      max_size_bytes: quota.max_size > 0 ? quota.max_size : 0,
      max_objects_limit: quota.max_objects > 0 ? quota.max_objects : 0,
      size_percent: evaluation.sizePercent,
      object_percent: evaluation.objectPercent,
      enforcement: evaluation.status,
      writes_blocked: evaluation.writesBlocked,
      is_hot: isHot,
      ...formatStorageClassQuotaFields(quota, formatSize)
    };
  });
}

export function getBlockedHotTiers(rows: StorageClassEnforcementRow[]): string[] {
  return rows
    .filter((row) => row.is_hot && row.writes_blocked)
    .map((row) => row.storage_class);
}

export const STORAGE_CLASS_MONITOR_COLUMNS = (): {
  name: string;
  prop: string;
  flexGrow: number;
}[] => [
  { name: $localize`Storage class`, prop: 'storage_class', flexGrow: 1 },
  { name: $localize`Placement`, prop: 'placement', flexGrow: 1 },
  { name: $localize`Used size`, prop: 'used_size', flexGrow: 1 },
  { name: $localize`Used objects`, prop: 'used_objects', flexGrow: 1 },
  { name: $localize`Quota enabled`, prop: 'enabled', flexGrow: 1 },
  { name: $localize`Maximum size`, prop: 'max_size', flexGrow: 1 },
  { name: $localize`Maximum objects`, prop: 'max_objects', flexGrow: 1 }
];
