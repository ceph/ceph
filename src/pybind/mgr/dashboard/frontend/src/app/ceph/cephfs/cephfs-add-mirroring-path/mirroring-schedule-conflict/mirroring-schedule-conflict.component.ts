import {
  AfterViewInit,
  Component,
  EventEmitter,
  inject,
  Input,
  OnChanges,
  Output,
  SimpleChanges,
  TemplateRef,
  ViewChild
} from '@angular/core';
import { take } from 'rxjs/operators';

import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { CdTableColumn } from '~/app/shared/models/cd-table-column';
import { SnapshotSchedule } from '~/app/shared/models/snapshot-schedule';
import { ExistingScheduleEntry } from '../mirroring-path.model';

@Component({
  selector: 'cd-mirroring-schedule-conflict',
  templateUrl: './mirroring-schedule-conflict.component.html',
  styleUrls: ['./mirroring-schedule-conflict.component.scss'],
  standalone: false
})
export class MirroringScheduleConflictComponent implements OnChanges, AfterViewInit {
  @Input() fsName = '';
  @Input() paths: string[] = [];

  @Output() existingSchedulesChange = new EventEmitter<ExistingScheduleEntry[]>();
  @Output() conflictActionChange = new EventEmitter<'keep' | 'replace' | 'individual'>();

  @ViewChild('actionTpl') actionTpl!: TemplateRef<any>;

  existingSchedules: ExistingScheduleEntry[] = [];
  scheduleConflictAction: 'keep' | 'replace' | 'individual' = 'keep';
  loading = false;

  pathActionOptions = [
    { content: $localize`Keep current schedules and add new`, value: 'keep' },
    { content: $localize`Replace existing schedules with the new schedule`, value: 'replace' }
  ];

  existingScheduleBaseColumns: CdTableColumn[] = [
    { prop: 'path', name: $localize`Path`, flexGrow: 4 },
    { prop: 'existingSchedule', name: $localize`Existing schedule`, flexGrow: 4 }
  ];
  existingScheduleActionColumns: CdTableColumn[] = [];

  private snapshotScheduleService = inject(CephfsSnapshotScheduleService);
  private viewReady = false;
  private pendingLoad = false;

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['paths'] || changes['fsName']) {
      this.queueLoad();
    }
  }

  ngAfterViewInit(): void {
    this.existingScheduleActionColumns = [
      { prop: 'path', name: $localize`Path`, flexGrow: 4 },
      { prop: 'existingSchedule', name: $localize`Existing schedule`, flexGrow: 4 },
      {
        prop: 'action',
        name: $localize`Action for existing schedule`,
        flexGrow: 4,
        cellTemplate: this.actionTpl
      }
    ];
    this.viewReady = true;
    if (this.pendingLoad) {
      this.loadSchedules();
    }
  }

  private queueLoad(): void {
    if (this.viewReady) {
      this.loadSchedules();
    } else {
      this.pendingLoad = true;
    }
  }

  onConflictActionChange(event: any): void {
    this.scheduleConflictAction = event?.value ?? event;
    this.conflictActionChange.emit(this.scheduleConflictAction);
  }

  setRowAction(rowId: string, action: 'keep' | 'replace'): void {
    const entry = this.existingSchedules.find((s) => s.id === rowId);
    if (entry) {
      entry.action = action;
      this.emitSchedules();
    }
  }

  private loadSchedules(): void {
    this.pendingLoad = false;
    if (!this.fsName || !this.paths.length) {
      this.existingSchedules = [];
      this.emitSchedules();
      return;
    }

    this.loading = true;
    this.snapshotScheduleService
      .getSnapshotSchedule('/', this.fsName, true)
      .pipe(take(1))
      .subscribe({
        next: (allSchedules) => {
          this.existingSchedules = this.paths.flatMap((selectedPath) => {
            const matching = allSchedules.filter((schedule) =>
              this.snapshotScheduleService.scheduleMatchesPath(schedule, selectedPath)
            );
            return this.mapScheduleEntries(selectedPath, matching);
          });
          this.emitSchedules();
          this.loading = false;
        },
        error: () => {
          this.existingSchedules = [];
          this.emitSchedules();
          this.loading = false;
        }
      });
  }

  private mapScheduleEntries(
    selectedPath: string,
    schedules: SnapshotSchedule[]
  ): ExistingScheduleEntry[] {
    const unique = new Map<string, SnapshotSchedule>();
    schedules.forEach((s) => {
      const key = `${s.path}@${s.schedule}`;
      if (!unique.has(key)) {
        unique.set(key, s);
      }
    });

    return Array.from(unique.values()).map((s) => ({
      id: `${selectedPath}@${s.schedule}`,
      path: selectedPath,
      existingSchedule: this.snapshotScheduleService.parseScheduleCopy(s.schedule),
      filesReplicating: s.created_count != null ? `${s.created_count} files` : '—',
      rawPath: s.path,
      schedule: s.schedule,
      start: s.start as unknown as string,
      retention: (s.retention as Record<string, number>) || {},
      subvol: s.subvol,
      group: s.group,
      fs: s.fs,
      action: 'keep' as const
    }));
  }

  private emitSchedules(): void {
    this.existingSchedulesChange.emit(this.existingSchedules);
  }
}
