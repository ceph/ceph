import { Component, DestroyRef, inject, OnInit, ViewChild } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';
import { Step } from 'carbon-components-angular';
import { from, Observable, of } from 'rxjs';
import { catchError, concatMap, finalize, map, switchMap, tap, toArray } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { SnapshotSchedule } from '~/app/shared/models/snapshot-schedule';
import { NotificationService } from '~/app/shared/services/notification.service';
import { MirroringPathUtils } from './mirroring-path-utils';
import {
  ExistingScheduleEntry,
  PathSubmitFailure,
  PathSubmitOutput
} from './mirroring-path.model';
import { MirroringPathsStepComponent } from './mirroring-paths-step/mirroring-paths-step.component';
import { MirroringScheduleConflictComponent } from './mirroring-schedule-conflict/mirroring-schedule-conflict.component';
import { CephfsSnapshotscheduleFormComponent } from '../cephfs-snapshotschedule-form/cephfs-snapshotschedule-form.component';

import { CEPHFS_MIRRORING_URL } from '~/app/shared/constants/cephfs.constant';

@Component({
  selector: 'cd-cephfs-add-mirroring-path',
  templateUrl: './cephfs-add-mirroring-path.component.html',
  styleUrls: ['./cephfs-add-mirroring-path.component.scss'],
  standalone: false
})
export class CephfsAddMirroringPathComponent implements OnInit {
  @ViewChild('pathsStep') pathsStep!: MirroringPathsStepComponent;
  @ViewChild('scheduleStep') scheduleStep?: CephfsSnapshotscheduleFormComponent;
  @ViewChild('scheduleConflict') scheduleConflict?: MirroringScheduleConflictComponent;

  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private cephfsService = inject(CephfsService);
  private snapshotScheduleService = inject(CephfsSnapshotScheduleService);
  private notificationService = inject(NotificationService);
  private destroyRef = inject(DestroyRef);

  fsName = '';
  fsId = 0;
  modalHeaderLabel = $localize`Filesystem mirroring`;
  title = $localize`Add mirroring path`;
  steps: Step[] = [
    { label: $localize`Paths`, invalid: false },
    { label: $localize`Schedule`, invalid: false },
    { label: $localize`Review`, invalid: false }
  ];
  isSubmitLoading = false;
  selectedPaths: string[] = [];
  existingSchedules: ExistingScheduleEntry[] = [];
  scheduleConflictAction: 'keep' | 'replace' | 'individual' = 'keep';

  get schedulePath(): string {
    return this.pathsStep?.getSubmitPaths()?.toAdd?.[0] ?? '';
  }

  ngOnInit(): void {
    this.fsId = Number(this.route.snapshot.paramMap.get('fsId'));
    const fsName = this.route.snapshot.paramMap.get('fsName') ?? '';
    try {
      this.fsName = decodeURIComponent(fsName);
    } catch {
      this.fsName = fsName;
    }
  }

  onStepChanged(event: { current: number }): void {
    if (event.current === 1) {
      this.selectedPaths = [...(this.pathsStep?.getSubmitPaths()?.toAdd ?? [])];
    }
  }

  onExistingSchedulesChange(entries: ExistingScheduleEntry[]): void {
    this.existingSchedules = entries;
  }

  onConflictActionChange(action: 'keep' | 'replace' | 'individual'): void {
    this.scheduleConflictAction = action;
  }

  onSubmit(): void {
    if (!this.pathsStep?.formGroup) {
      return;
    }

    if (this.scheduleConflict) {
      this.scheduleConflictAction = this.scheduleConflict.scheduleConflictAction;
      this.existingSchedules = this.scheduleConflict.existingSchedules;
    }

    this.isSubmitLoading = true;

    this.pathsStep
      .refreshTrackedPaths()
      .pipe(
        switchMap(() => this.addMirrorDirectories()),
        finalize(() => (this.isSubmitLoading = false)),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe((outcome) => {
        if (outcome?.succeeded?.length) {
          this.closeTearsheet(true);
        }
      });
  }

  onCancel(): void {
    this.closeTearsheet(false);
  }

  private addMirrorDirectories(): Observable<PathSubmitOutput> {
    const { toAdd, alreadyMirrored } = this.pathsStep.getSubmitPaths();
    const emptyOutcome: PathSubmitOutput = {
      failed: [],
      scheduleFailed: [],
      alreadyMirrored,
      skippedByServer: [],
      succeeded: []
    };

    if (!toAdd.length) {
      this.showSubmitSummary(emptyOutcome);
      return of(emptyOutcome);
    }

    const skippedByServer: string[] = [];
    const failed: PathSubmitFailure[] = [];

    return from(toAdd).pipe(
      concatMap((path) => this.addSingleMirrorPath(path, failed, skippedByServer)),
      toArray(),
      switchMap((results) => {
        const succeeded = results.filter((p): p is string => !!p);
        const { toAdd, alreadyMirrored } = this.pathsStep.getSubmitPaths();
        const mirrorReadyPaths = [
          ...new Set(
            toAdd.filter((path) => succeeded.includes(path) || alreadyMirrored.includes(path))
          )
        ];
        return this.loadExistingSchedulesForPaths(toAdd).pipe(
          switchMap(() => this.handleSchedules(mirrorReadyPaths)),
          map((scheduleFailed) => ({
            failed,
            scheduleFailed,
            alreadyMirrored,
            skippedByServer,
            succeeded
          }))
        );
      }),
      tap((outcome) => this.showSubmitSummary(outcome))
    );
  }

  private addSingleMirrorPath(
    path: string,
    failed: PathSubmitFailure[],
    skippedByServer: string[]
  ): Observable<string | null> {
    if (!this.pathsStep.getSubmitPaths().toAdd.includes(path)) {
      skippedByServer.push(path);
      return of(null);
    }

    return this.cephfsService.addMirrorDirectory(this.fsName, path).pipe(
      tap(() => this.pathsStep.addTrackedPath(path)),
      map(() => path),
      catchError((error) => {
        const detail =
          error?.error?.detail ||
          error?.message ||
          $localize`Failed to add mirroring path '${path}'`;
        if (MirroringPathUtils.isAlreadyTrackedMirrorError(detail)) {
          error?.preventDefault?.();
          this.pathsStep.addTrackedPath(path);
          return of(path);
        }
        failed.push({ path, detail });
        return of(null);
      })
    );
  }

  private handleSchedules(mirrorReadyPaths: string[]): Observable<PathSubmitFailure[]> {
    const pathsWithoutSchedules = mirrorReadyPaths.filter((p) => !this.pathHasExistingSchedule(p));

    if (this.scheduleConflictAction === 'keep') {
      const skipped = mirrorReadyPaths.filter((p) => this.pathHasExistingSchedule(p));
      if (skipped.length) {
        this.notificationService.show(
          NotificationType.info,
          $localize`Keeping existing snapshot schedules for ${skipped.length} path(s).`,
          skipped.join('\n')
        );
      }
      return this.createSnapshotSchedules(pathsWithoutSchedules);
    }

    if (this.scheduleConflictAction === 'replace') {
      return this.deleteExistingSchedules(mirrorReadyPaths).pipe(
        switchMap(() => this.createSnapshotSchedules(mirrorReadyPaths))
      );
    }

    const replaceEntries = this.existingSchedules.filter((s) => s.action === 'replace');
    const keepPaths = new Set(
      this.existingSchedules.filter((s) => s.action === 'keep').map((s) => s.path)
    );
    const pathsToCreate = mirrorReadyPaths.filter((p) => !keepPaths.has(p));

    const preAction = replaceEntries.length
      ? this.deleteScheduleEntries(replaceEntries)
      : of(undefined);

    return preAction.pipe(switchMap(() => this.createSnapshotSchedules(pathsToCreate)));
  }

  private pathHasExistingSchedule(path: string): boolean {
    const normalized = MirroringPathUtils.normalizePath(path);
    return this.existingSchedules.some(
      (schedule) => MirroringPathUtils.normalizePath(schedule.path) === normalized
    );
  }

  private loadExistingSchedulesForPaths(paths: string[]): Observable<void> {
    if (!paths.length) {
      this.existingSchedules = [];
      return of(undefined);
    }

    return this.snapshotScheduleService.getSnapshotSchedule('/', this.fsName, true).pipe(
      map((allSchedules) => {
        this.existingSchedules = paths.flatMap((selectedPath) => {
          const matching = allSchedules.filter((schedule) =>
            this.snapshotScheduleService.scheduleMatchesPath(schedule, selectedPath)
          );
          return this.mapScheduleEntries(selectedPath, matching);
        });
      }),
      map(() => undefined),
      catchError(() => {
        this.existingSchedules = [];
        return of(undefined);
      })
    );
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

  private deleteExistingSchedules(paths: string[]): Observable<void> {
    const normalizedPaths = new Set(paths.map((path) => MirroringPathUtils.normalizePath(path)));
    return this.deleteScheduleEntries(
      this.existingSchedules.filter((schedule) =>
        normalizedPaths.has(MirroringPathUtils.normalizePath(schedule.path))
      )
    );
  }

  private deleteScheduleEntries(entries: ExistingScheduleEntry[]): Observable<void> {
    if (!entries.length) {
      return of(undefined);
    }

    return from(entries).pipe(
      concatMap((s) => {
        const retentionPolicy = Object.keys(s.retention).length
          ? Object.entries(s.retention)
              .map(([freq, count]) => `${count}-${freq}`)
              .join('|')
          : undefined;
        return this.snapshotScheduleService
          .delete({
            fs: s.fs,
            path: s.rawPath,
            schedule: s.schedule,
            start: s.start,
            retentionPolicy,
            subvol: s.subvol,
            group: s.group
          })
          .pipe(
            catchError((error) => {
              const detail =
                error?.error?.detail || error?.message || $localize`Failed to delete schedule`;
              this.notificationService.show(
                NotificationType.warning,
                $localize`Failed to remove existing schedule for '${s.path}'`,
                detail
              );
              return of(undefined);
            })
          );
      }),
      toArray(),
      map(() => undefined)
    );
  }

  private createSnapshotSchedules(paths: string[]): Observable<PathSubmitFailure[]> {
    if (!this.scheduleStep || !paths.length) {
      return of([]);
    }

    const uniquePaths = [...new Set(paths.map((path) => MirroringPathUtils.normalizePath(path)))].filter(
      Boolean
    );

    return from(uniquePaths).pipe(
      concatMap((path) =>
        this.snapshotScheduleService.create(this.scheduleStep.buildCreatePayload(path)).pipe(
          map(() => null),
          catchError((error) => {
            const detail =
              error?.error?.detail ||
              error?.message ||
              $localize`Failed to create snapshot schedule for '${path}'`;
            if (this.snapshotScheduleService.isScheduleExistsError(error)) {
              error?.preventDefault?.();
              return of({
                path,
                detail: $localize`A snapshot schedule already exists for '${path}'.`
              });
            }
            this.notificationService.show(
              NotificationType.error,
              $localize`Failed to create snapshot schedule`,
              detail
            );
            return of({ path, detail });
          })
        )
      ),
      toArray(),
      map((results) => results.filter((result): result is PathSubmitFailure => !!result))
    );
  }

  private closeTearsheet(reload: boolean): void {
    this.router.navigate([CEPHFS_MIRRORING_URL, { outlets: { modal: null } }], {
      state: reload ? { reload: true } : undefined
    });
  }

  private showSubmitSummary(outcome: PathSubmitOutput): void {
    const { failed, scheduleFailed, alreadyMirrored, skippedByServer, succeeded } = outcome;
    const serverOnlySkipped = skippedByServer.filter((path) => !alreadyMirrored.includes(path));

    if (alreadyMirrored.length) {
      this.notificationService.show(
        NotificationType.warning,
        $localize`Skipped ${alreadyMirrored.length} path(s) that are already mirrored.`
      );
    }

    if (serverOnlySkipped.length) {
      this.notificationService.show(
        NotificationType.warning,
        $localize`Skipped ${serverOnlySkipped.length} path(s) that are already mirrored.`
      );
    }

    if (succeeded.length) {
      if (succeeded.length === 1) {
        this.notificationService.show(
          NotificationType.success,
          $localize`Mirroring path '${succeeded[0]}' added to ${this.fsName}`
        );
      } else {
        this.notificationService.show(
          NotificationType.success,
          $localize`Added ${succeeded.length} mirroring paths to ${this.fsName}`,
          succeeded.join('\n')
        );
      }
    }

    if (scheduleFailed.length) {
      const title =
        scheduleFailed.length === 1
          ? $localize`Failed to create snapshot schedule`
          : $localize`Failed to create ${scheduleFailed.length} snapshot schedules`;
      const message = scheduleFailed.map(({ path, detail }) => `${path}: ${detail}`).join('\n');
      this.notificationService.show(NotificationType.warning, title, message);
    }

    if (!failed.length) {
      return;
    }

    const title =
      failed.length === 1
        ? $localize`Failed to add mirroring path`
        : $localize`Failed to add ${failed.length} mirroring paths`;
    const message = failed.map(({ path, detail }) => `${path}: ${detail}`).join('\n');
    this.notificationService.show(NotificationType.error, title, message);
  }
}
