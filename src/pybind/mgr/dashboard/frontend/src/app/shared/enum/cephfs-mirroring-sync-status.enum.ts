export enum MirroringSyncStatus {
  SYNCING = 'syncing',
  IDLE = 'idle',
  FAILED = 'failed',
  COMPLETED = 'completed',
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
