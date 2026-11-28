import { BucketTieringUtils } from './rgw-bucket-tiering';

describe('BucketTieringUtils zone lookup', () => {
  const zones: any[] = [
    {
      name: 'zone-a',
      placement_pools: [
        { key: 'default-placement', val: { storage_classes: { SC: { data_pool: 'pool-a' } } } }
      ]
    },
    {
      name: 'zone-b',
      placement_pools: [
        { key: 'default-placement', val: { storage_classes: { SC: { data_pool: 'pool-b' } } } }
      ]
    }
  ];
  const selected = { placement_target: 'default-placement', storage_class: 'SC' };

  it('should scope the lookup to the zonegroup members', () => {
    expect(BucketTieringUtils.getZoneInfoHelper(zones, selected, ['zone-b'])).toEqual({
      zone_name: 'zone-b',
      data_pool: 'pool-b'
    });
  });

  it('should return empty info when the zonegroup has no matching zone', () => {
    expect(BucketTieringUtils.getZoneInfoHelper(zones, selected, [])).toEqual({
      zone_name: '',
      data_pool: ''
    });
  });

  it('should resolve zone names from string or object members', () => {
    const zonegroups: any[] = [
      { name: 'zg1', zones: ['zone-a'] },
      { name: 'zg2', zones: [{ name: 'zone-b' }] }
    ];
    expect(BucketTieringUtils.getZonegroupZoneNames(zonegroups, 'zg1')).toEqual(['zone-a']);
    expect(BucketTieringUtils.getZonegroupZoneNames(zonegroups, 'zg2')).toEqual(['zone-b']);
    expect(BucketTieringUtils.getZonegroupZoneNames(zonegroups, 'none')).toBeUndefined();
  });
});
