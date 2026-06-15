import { NgbTimeStruct } from '@ng-bootstrap/ng-bootstrap';
import { ICON_TYPE } from '~/app/shared/enum/icons.enum';

export interface SnapshotSchedule {
  fs?: string;
  subvol?: string;
  path: string;
  rel_path?: string;
  schedule: string;
  scheduleCopy?: string;
  retention?: Record<string, number> | string;
  retentionCopy?: string[];
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

export interface SnapshotScheduleCreatePayload {
  fs: string;
  path: string;
  snap_schedule: string;
  start: string;
  retention_policy?: string;
  subvol?: string;
  group?: string;
}

export function parseSchedule(interval: number, frequency: string): string {
  return `${interval}${frequency}`;
}

export function parseRetentionPolicies(retentionPolicies: RetentionPolicy[]): string | undefined {
  const value = retentionPolicies
    ?.filter((policy) => policy?.retentionInterval != null && policy?.retentionFrequency != null)
    ?.map((policy) => `${policy.retentionInterval}-${policy.retentionFrequency}`)
    .join('|');

  return value || undefined;
}

export function parseScheduleStart(startDate?: string): string {
  if (!startDate) {
    return new Date().toISOString().slice(0, 19);
  }

  return new Date(startDate.replace(/\//g, '-').replace(' ', 'T')).toISOString().slice(0, 19);
}

export function buildSnapshotScheduleCreatePayload(params: {
  fs: string;
  path: string;
  repeatInterval: number;
  repeatFrequency: string;
  startDate?: string;
  retentionPolicies?: RetentionPolicy[];
  subvol?: string;
  group?: string;
}): SnapshotScheduleCreatePayload {
  const payload: SnapshotScheduleCreatePayload = {
    fs: params.fs,
    path: params.path,
    snap_schedule: parseSchedule(params.repeatInterval, params.repeatFrequency),
    start: parseScheduleStart(params.startDate)
  };

  const retentionPolicy = parseRetentionPolicies(params.retentionPolicies ?? []);
  if (retentionPolicy) {
    payload.retention_policy = retentionPolicy;
  }

  if (params.subvol) {
    payload.subvol = params.subvol;
    if (params.group) {
      payload.group = params.group;
    }
  }

  return payload;
}
