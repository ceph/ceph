import { NgbDateStruct, NgbTimeStruct } from '@ng-bootstrap/ng-bootstrap';

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

export interface SnapshotScheduleFormValue {
  directory: string;
  startDate: NgbDateStruct;
  startTime: NgbTimeStruct;
  repeatInterval: number;
  repeatFrequency: string;
  retentionPolicies: RetentionPolicy[];
}

export interface RetentionPolicy {
  retentionInterval: number;
  retentionFrequency: string;
}
