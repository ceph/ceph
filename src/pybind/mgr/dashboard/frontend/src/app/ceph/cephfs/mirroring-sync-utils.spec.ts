import { MirroringSyncUtils } from './mirroring-sync-utils';
import { MirroringFsSyncInfo } from '~/app/shared/models/cephfs.model';

describe('MirroringSyncUtils', () => {
  it('extractLatestSync counts syncing paths and picks latest snapshot', () => {
    const sync = MirroringSyncUtils.extractLatestSync({
      metrics: {
        '/old': {
          peer: {
            p1: {
              state: 'idle',
              last_synced_snap: { name: 'old', sync_bytes: '1 B', sync_time_stamp: '1s' },
              metrics_updated_at: 100
            }
          }
        },
        '/new': {
          peer: {
            p2: {
              state: 'syncing',
              last_synced_snap: { name: 'new', sync_bytes: '2 B', sync_time_stamp: '2s' },
              metrics_updated_at: 200
            }
          }
        }
      }
    });

    expect(sync.syncingPaths).toBe(1);
    expect(sync.info.snapName).toBe('new');
    expect(sync.info.path).toBe('/new');
    expect(sync.info.syncedAt).toBe(200);
  });

  it('isNewerMirrorSync prefers metrics_updated_at over sync timestamp', () => {
    expect(MirroringSyncUtils.isNewerMirrorSync('1s', 300, '9s', 200)).toBe(true);
    expect(MirroringSyncUtils.isNewerMirrorSync('9s', 100, '1s', 200)).toBe(false);
  });

  it('mirrorMetricsUpdatedAtToEpoch parses valid values and rejects invalid ones', () => {
    expect(MirroringSyncUtils.mirrorMetricsUpdatedAtToEpoch(1_700_000_000.9)).toBe(1_700_000_000);
    expect(MirroringSyncUtils.mirrorMetricsUpdatedAtToEpoch('1234.5')).toBe(1234);
    expect(MirroringSyncUtils.mirrorMetricsUpdatedAtToEpoch('')).toBeNull();
    expect(MirroringSyncUtils.mirrorMetricsUpdatedAtToEpoch(0)).toBeNull();
  });

  it('emptySyncInfo returns placeholder values', () => {
    const info: MirroringFsSyncInfo = MirroringSyncUtils.emptySyncInfo();
    expect(info.bytesSynced).toBe('-');
    expect(info.syncedAt).toBeNull();
  });
});
