import { MirroringFsSyncInfo, MirrorStatusResponse } from '~/app/shared/models/cephfs.model';
import { FormatterService } from '~/app/shared/services/formatter.service';

export class MirroringSyncUtils {
  static emptySyncInfo(): MirroringFsSyncInfo {
    return {
      bytesSynced: '-',
      path: '',
      snapName: '',
      syncedAt: null
    };
  }

  static extractLatestSync(status: MirrorStatusResponse): {
    syncingPaths: number;
    info: MirroringFsSyncInfo;
  } {
    let syncingPaths = 0;
    let latestSyncTime = 0;
    let latestSnapName = '';
    let latestSyncPath = '';
    let totalBytesSynced = 0;
    let hasBytesSynced = false;

    for (const [dirPath, dirMetrics] of Object.entries(status.metrics ?? {})) {
      for (const dir of Object.values(dirMetrics.peer ?? {})) {
        if (dir.state === 'syncing') {
          syncingPaths++;
        }

        const snap = dir.last_synced_snap;
        if (!snap) {
          continue;
        }

        const parsedBytes = MirroringSyncUtils.parseSyncBytes(snap.sync_bytes);
        if (parsedBytes !== null) {
          totalBytesSynced += parsedBytes;
          hasBytesSynced = true;
        }

        const syncTime = MirroringSyncUtils.parseSyncTimeStamp(snap.sync_time_stamp);
        if (syncTime !== null && syncTime >= latestSyncTime) {
          latestSyncTime = syncTime;
          latestSnapName = snap.name ?? '';
          latestSyncPath = dirPath;
        }
      }
    }

    return {
      syncingPaths,
      info: {
        bytesSynced: hasBytesSynced ? MirroringSyncUtils.formatSyncBytes(totalBytesSynced) : '-',
        path: latestSyncPath,
        snapName: latestSnapName,
        syncedAt: latestSyncTime || null
      }
    };
  }

  static parseSyncTimeStamp(value: number | string | undefined | null): number | null {
    if (value === undefined || value === null || value === '') {
      return null;
    }
    if (typeof value === 'number') {
      return Number.isFinite(value) && value > 0 ? value : null;
    }

    const str = String(value).trim();
    if (!str) {
      return null;
    }

    if (str.includes('T')) {
      const ms = Date.parse(str.replace(/([+-]\d{2})(\d{2})$/, '$1:$2'));
      if (!Number.isFinite(ms)) {
        return null;
      }
      return ms / 1000;
    }

    const epoch = parseFloat(str);
    return Number.isFinite(epoch) && epoch > 0 ? epoch : null;
  }

  static parseSyncBytes(value: number | string | undefined | null): number | null {
    if (value === undefined || value === null || value === '') {
      return null;
    }
    if (typeof value === 'number') {
      return Number.isFinite(value) ? value : null;
    }
    return new FormatterService().toBytes(String(value).replace(/\s+/g, ''), null);
  }

  static formatSyncBytes(bytes: number): string {
    return new FormatterService().formatToBinary(bytes, false, 2);
  }
}
