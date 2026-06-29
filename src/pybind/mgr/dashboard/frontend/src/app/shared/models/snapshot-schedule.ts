import { NgbTimeStruct } from '@ng-bootstrap/ng-bootstrap';
import { ICON_TYPE } from '~/app/shared/enum/icons.enum';

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

export interface MirrorPathSchedule extends SnapshotSchedule {
  retention?: Record<string, number>;
  retentionCopy?: string[];
  scheduleCopy?: string;
  nextSync?: string;
  scheduleText?: string;
  retentionText?: string;
  statusClass?: string;
  statusLabel?: string;
  statusIcon?: keyof typeof ICON_TYPE;
  removeId?: string;
}

export interface SnapshotScheduleFormValue {
  directory: string;
  startDate: string;
  startTime: NgbTimeStruct;
  repeatInterval: number;
  repeatFrequency: string;
  retentionPolicies: RetentionPolicy[];
}

export interface RetentionPolicy {
  retentionInterval: number;
  retentionFrequency: string;
}
