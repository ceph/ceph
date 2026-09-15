import { MirroringSyncUtils } from './mirroring-sync-utils';
import { MirroringFsSyncInfo } from '~/app/shared/models/cephfs.model';

describe('MirroringSyncUtils', () => {
  it('extractLatestSync counts syncing paths and picks latest snapshot by sync_time_stamp', () => {
    const sync = MirroringSyncUtils.extractLatestSync({
      metrics: {
        '/old': {
          peer: {
            p1: {
              state: 'idle',
              last_synced_snap: {
                name: 'old',
                sync_bytes: '1 B',
                sync_time_stamp: '1786358053.364396s'
              }
            }
          }
        },
        '/new': {
          peer: {
            p2: {
              state: 'syncing',
              last_synced_snap: {
                name: 'new',
                sync_bytes: '2 B',
                sync_time_stamp: '1786362048.305584s'
              }
            }
          }
        }
      }
    });

    expect(sync.syncingPaths).toBe(1);
    expect(sync.info.snapName).toBe('new');
    expect(sync.info.path).toBe('/new');
    expect(sync.info.bytesSynced).toBe('3 B');
    expect(sync.info.syncedAt).toBe(1786362048.305584);
  });

  it('extractLatestSync sums sync_bytes across all mirror paths', () => {
    const sync = MirroringSyncUtils.extractLatestSync({
      metrics: {
        '/dir1': {
          peer: {
            'peer-uuid': {
              state: 'idle',
              last_synced_snap: {
                name: 'snap1',
                sync_bytes: '1.00 MiB',
                sync_time_stamp: '1786358053.364396s'
              }
            }
          }
        },
        '/dir2': {
          peer: {
            'peer-uuid': {
              state: 'idle',
              last_synced_snap: {
                name: 'snap2',
                sync_bytes: '2.00 MiB',
                sync_time_stamp: '1786362048.305584s'
              }
            }
          }
        },
        '/dir3': {
          peer: {
            'peer-uuid': {
              state: 'syncing',
              last_synced_snap: {
                name: 'snap3',
                sync_bytes: 1048576,
                sync_time_stamp: 1786443055.4675815
              }
            }
          }
        }
      }
    });

    expect(sync.info.bytesSynced).toBe('4 MiB');
    expect(sync.syncingPaths).toBe(1);
    expect(sync.info.path).toBe('/dir3');
    expect(sync.info.syncedAt).toBe(1786443055.4675815);
  });

  it('emptySyncInfo returns placeholder values', () => {
    const info: MirroringFsSyncInfo = MirroringSyncUtils.emptySyncInfo();
    expect(info.bytesSynced).toBe('-');
    expect(info.syncedAt).toBeNull();
  });

  it('parseSyncTimeStamp handles ISO-8601 and epoch sync_time_stamp values', () => {
    expect(MirroringSyncUtils.parseSyncTimeStamp('2026-08-12T13:32:21.383628+0000')).toBe(
      1786541541.383
    );
    expect(MirroringSyncUtils.parseSyncTimeStamp('1786358053.364396s')).toBe(1786358053.364396);
    expect(MirroringSyncUtils.parseSyncTimeStamp(1786443055.4675815)).toBe(1786443055.4675815);
    expect(MirroringSyncUtils.parseSyncTimeStamp(undefined)).toBeNull();
  });

  it('extractLatestSync picks the latest snapshot from ISO sync_time_stamp values', () => {
    const sync = MirroringSyncUtils.extractLatestSync({
      metrics: {
        '/old': {
          peer: {
            p1: {
              state: 'idle',
              last_synced_snap: {
                name: 'old',
                sync_time_stamp: '2026-08-12T13:00:00.000000+0000'
              }
            }
          }
        },
        '/new': {
          peer: {
            p2: {
              state: 'idle',
              last_synced_snap: {
                name: 'new',
                sync_time_stamp: '2026-08-12T13:32:21.383628+0000'
              }
            }
          }
        }
      }
    });

    expect(sync.info.snapName).toBe('new');
    expect(sync.info.path).toBe('/new');
    expect(sync.info.syncedAt).toBe(1786541541.383);
  });

  it('parseSyncBytes handles numeric and human-readable values', () => {
    expect(MirroringSyncUtils.parseSyncBytes('1.00 MiB')).toBe(1048576);
    expect(MirroringSyncUtils.parseSyncBytes(2048)).toBe(2048);
    expect(MirroringSyncUtils.parseSyncBytes(undefined)).toBeNull();
  });
});
