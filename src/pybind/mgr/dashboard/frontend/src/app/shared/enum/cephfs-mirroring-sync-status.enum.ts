export enum MirroringSyncStatus {
  SYNCING = 'syncing',
  IDLE = 'idle',
  ERROR = 'error',
  NONE = 'none'
}

export enum MirroringSnapshotStatus {
  IN_PROGRESS = 'in-progress',
  REPLICATED = 'replicated',
  PENDING = 'pending',
  FAILED = 'failed'
}

export enum MirroringSnapshotSection {
  CURRENT = 'current',
  SYNCED = 'synced'
}
