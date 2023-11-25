export interface SnapshotSchedule {
  fs?: string;
  subvol?: string;
  path: string;
  rel_path?: string;
  schedule: string;
  retention?: Record<string, number> | string;
  start: Date;
  created: Date;
  first?: string;
  last?: string;
  last_pruned?: string;
  created_count?: number;
  pruned_count?: number;
  active: boolean;
  status: 'Active' | 'Inactive';
}
