import {
  DUMMY_BUCKET_STORAGE_CLASS_STATS,
  DUMMY_STORAGE_CLASS_QUOTAS,
  DUMMY_USER_STORAGE_CLASS_STATS,
  extractBucketStorageClassStats,
  extractUserStorageClassStats,
  parsePlacementClassKey,
  toStorageClassMonitorRows,
  toStorageClassQuotaDisplayRows,
  toStorageClassUsageRows
} from './rgw-storage-class-stats';

describe('rgw-storage-class-stats', () => {
  it('should parse placement::class keys from user stats', () => {
    expect(parsePlacementClassKey('default-placement::STANDARD')).toEqual({
      placement: 'default-placement',
      storage_class: 'STANDARD'
    });
    expect(parsePlacementClassKey('HDD')).toEqual({
      placement: '',
      storage_class: 'HDD'
    });
  });

  it('should map user stats.storage-classes to rows', () => {
    const rows = toStorageClassUsageRows({
      'default-placement::STANDARD': {
        size: 21422080,
        size_actual: 21422080,
        num_objects: 1
      },
      'default-placement::HDD': {
        size: 42844160,
        size_actual: 42844160,
        num_objects: 2
      }
    });
    expect(rows).toEqual([
      {
        storage_class: 'STANDARD',
        placement: 'default-placement',
        size: 21422080,
        size_actual: 21422080,
        num_objects: 1
      },
      {
        storage_class: 'HDD',
        placement: 'default-placement',
        size: 42844160,
        size_actual: 42844160,
        num_objects: 2
      }
    ]);
  });

  it('should map bucket usage rgw.storage-classes arrays to rows', () => {
    const rows = toStorageClassUsageRows([
      { name: 'HDD', size: 10, size_actual: 10, num_objects: 2 },
      { name: 'STANDARD', size: 5, size_actual: 5, num_objects: 1 }
    ]);
    expect(rows.map((row) => row.storage_class)).toEqual(['HDD', 'STANDARD']);
  });

  it('should fall back to dummy usage when the API omits storage-class stats', () => {
    const rows = toStorageClassUsageRows(undefined, DUMMY_USER_STORAGE_CLASS_STATS);
    expect(rows.map((row) => row.storage_class)).toEqual(['STANDARD', 'HDD']);
  });

  it('should read user and bucket payloads from the #66501 field names', () => {
    expect(
      extractUserStorageClassStats({
        'stats.storage-classes': DUMMY_USER_STORAGE_CLASS_STATS
      })
    ).toBe(DUMMY_USER_STORAGE_CLASS_STATS);

    expect(
      extractBucketStorageClassStats({
        usage: { 'rgw.storage-classes': DUMMY_BUCKET_STORAGE_CLASS_STATS }
      })
    ).toBe(DUMMY_BUCKET_STORAGE_CLASS_STATS);
  });

  it('should merge usage from #66501 with dummy quotas when limits are missing', () => {
    const rows = toStorageClassMonitorRows(DUMMY_USER_STORAGE_CLASS_STATS, undefined, (bytes) =>
      `${bytes} B`
    );
    expect(rows).toEqual([
      {
        storage_class: 'STANDARD',
        placement: 'default-placement',
        used_size: '21422080 B',
        used_objects: 1,
        enabled: 'Yes',
        max_size: '53687091200 B',
        max_objects: 10000
      },
      {
        storage_class: 'HDD',
        placement: 'default-placement',
        used_size: '42844160 B',
        used_objects: 2,
        enabled: 'Yes',
        max_size: '214748364800 B',
        max_objects: 50000
      }
    ]);
  });

  it('should show dummy quota rows when the API omits storage class quotas', () => {
    expect(toStorageClassQuotaDisplayRows(undefined, (bytes) => `${bytes} B`)).toEqual(
      DUMMY_STORAGE_CLASS_QUOTAS.map((quota) => ({
        storage_class: quota.storage_class,
        enabled: 'Yes',
        max_size: `${quota.max_size} B`,
        max_objects: quota.max_objects
      }))
    );
  });
});
