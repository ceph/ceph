import { TemplateRef } from '@angular/core';

import { CellTemplate } from '~/app/shared/enum/cell-template.enum';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { SnapshotSchedule } from '~/app/shared/models/snapshot-schedule';
import { MirroringPathSelection } from '../mirroring-path.model';
import { normalizeMirroringPath } from '../mirroring-path-utils';
import {
  EXISTING_SCHEDULE_ACTION,
  EXISTING_SCHEDULE_HANDLING,
  ExistingScheduleAction,
  ExistingScheduleHandling,
  PathScheduleConflictRow
} from './mirroring-schedule-step.model';

export const EMPTY_SCHEDULE_LABEL = '—';

export function toGlobalConflictAction(
  handling: ExistingScheduleHandling
): ExistingScheduleAction {
  return handling === EXISTING_SCHEDULE_HANDLING.REPLACE
    ? EXISTING_SCHEDULE_ACTION.REPLACE
    : EXISTING_SCHEDULE_ACTION.KEEP_AND_ADD;
}

export function createConflictTableColumns(actionTpl?: TemplateRef<unknown>): CdTableColumn[] {
  const columns: CdTableColumn[] = [
    {
      prop: 'path',
      name: $localize`Path`,
      flexGrow: 4,
      cellTransformation: CellTemplate.path
    },
    { prop: 'scheduleCopy', name: $localize`Existing schedule`, flexGrow: 2 },
    { prop: 'filesReplicating', name: $localize`Files replicating`, flexGrow: 2 }
  ];

  if (actionTpl) {
    columns.push({
      prop: 'action',
      name: $localize`Action for existing schedule`,
      flexGrow: 2.5,
      cellTemplate: actionTpl
    });
  }

  return columns;
}

export function matchSchedulesForSelection(
  allSchedules: SnapshotSchedule[],
  selection: MirroringPathSelection
): SnapshotSchedule[] {
  const normalizedSelected = normalizeMirroringPath(selection.path);

  return allSchedules.filter((schedule) => {
    const schedulePath = normalizeMirroringPath(schedule.path || '');

    if (!schedulePath) {
      return false;
    }

    if (schedulePath === normalizedSelected) {
      return true;
    }

    if (
      schedulePath.startsWith(`${normalizedSelected}/`) ||
      normalizedSelected.startsWith(`${schedulePath}/`)
    ) {
      return true;
    }

    return !!(selection.subvol && schedule.subvol === selection.subvol);
  });
}

export function formatScheduleLabel(
  schedule: SnapshotSchedule,
  parseScheduleCopy: (schedule: string) => string
): string {
  if (schedule.scheduleCopy) {
    return schedule.scheduleCopy;
  }
  if (schedule.schedule) {
    return parseScheduleCopy(schedule.schedule);
  }
  return '';
}

export function formatExistingSchedules(
  schedules: SnapshotSchedule[],
  parseScheduleCopy: (schedule: string) => string
): string {
  if (!schedules.length) {
    return EMPTY_SCHEDULE_LABEL;
  }

  const labels = [
    ...new Set(
      schedules
        .map((schedule) => formatScheduleLabel(schedule, parseScheduleCopy))
        .filter(Boolean)
    )
  ];

  return labels.length ? labels.join(', ') : EMPTY_SCHEDULE_LABEL;
}

export function buildPathScheduleRows(
  selections: MirroringPathSelection[],
  allSchedules: SnapshotSchedule[],
  defaultAction: ExistingScheduleAction,
  parseScheduleCopy: (schedule: string) => string
): PathScheduleConflictRow[] {
  return selections.map((selection) => ({
    id: selection.path,
    path: selection.path,
    scheduleCopy: formatExistingSchedules(
      matchSchedulesForSelection(allSchedules, selection),
      parseScheduleCopy
    ),
    filesReplicating: EMPTY_SCHEDULE_LABEL,
    action: defaultAction
  }));
}

export function hasExistingSchedules(rows: PathScheduleConflictRow[]): boolean {
  return rows.some((row) => row.scheduleCopy !== EMPTY_SCHEDULE_LABEL);
}
