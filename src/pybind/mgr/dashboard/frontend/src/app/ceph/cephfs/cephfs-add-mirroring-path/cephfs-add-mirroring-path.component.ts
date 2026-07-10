import { Component, DestroyRef, inject, OnInit, ViewChild } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';
import { Step } from 'carbon-components-angular';
import { from, Observable, of } from 'rxjs';
import { catchError, concatMap, finalize, map, switchMap, tap, toArray } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { URLVerbs } from '~/app/shared/constants/app.constants';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { FinishedTask } from '~/app/shared/models/finished-task';
import { NotificationService } from '~/app/shared/services/notification.service';
import { TaskWrapperService } from '~/app/shared/services/task-wrapper.service';
import { MirroringPathUtils } from './mirroring-path-utils';
import { PathSubmitFailure, PathSubmitOutput } from './mirroring-path.model';
import { MirroringPathsStepComponent } from './mirroring-paths-step/mirroring-paths-step.component';
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

  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private cephfsService = inject(CephfsService);
  private snapshotScheduleService = inject(CephfsSnapshotScheduleService);
  private taskWrapper = inject(TaskWrapperService);
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

  get schedulePath(): string {
    return this.pathsStep?.getSelectedPaths()?.[0] ?? '';
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

  onSubmit(): void {
    const pathsStep = this.pathsStep;
    if (!pathsStep?.formGroup) {
      return;
    }

    this.isSubmitLoading = true;

    pathsStep
      .refreshTrackedPaths()
      .pipe(
        switchMap(() => {
          const { toAdd, alreadyMirrored } = pathsStep.getSubmitPaths();
          const selectedPaths = pathsStep.getSelectedPaths();
          const pathsToMirror = [...toAdd];

          if (!selectedPaths.length) {
            this.showSubmitSummary({
              failed: [],
              alreadyMirrored,
              skippedByServer: [],
              succeeded: []
            });
            return of({
              failed: [],
              alreadyMirrored,
              skippedByServer: [],
              succeeded: [],
              schedulesCreated: 0
            });
          }

          if (!pathsToMirror.length) {
            return this.createSnapshotSchedules(selectedPaths).pipe(
              map((schedulesCreated) => ({
                failed: [],
                alreadyMirrored,
                skippedByServer: [],
                succeeded: [],
                schedulesCreated
              })),
              tap((outcome) => this.showSubmitSummary(outcome))
            );
          }

          const skippedByServer: string[] = [];
          const failed: PathSubmitFailure[] = [];

          return from(pathsToMirror).pipe(
            concatMap((path) =>
              this.cephfsService.addMirrorDirectory(this.fsName, path).pipe(
                tap(() => pathsStep.addTrackedPath(path)),
                map(() => path),
                catchError((error) => {
                  const detail =
                    error?.error?.detail ||
                    error?.message ||
                    $localize`Failed to add mirroring path '${path}'`;
                  if (MirroringPathUtils.isAlreadyTrackedMirrorError(detail)) {
                    pathsStep.addTrackedPath(path);
                    skippedByServer.push(path);
                    return of(null);
                  }
                  failed.push({ path, detail });
                  return of(null);
                })
              )
            ),
            toArray(),
            switchMap((results) => {
              const succeeded = results.filter((path): path is string => !!path);
              const mirrorFailed = new Set(failed.map(({ path }) => path));
              const pathsForSchedule = [
                ...new Set(selectedPaths.filter((path) => !mirrorFailed.has(path)))
              ];
              return this.createSnapshotSchedules(pathsForSchedule).pipe(
                map((schedulesCreated) => ({
                  failed,
                  alreadyMirrored,
                  skippedByServer,
                  succeeded,
                  schedulesCreated
                }))
              );
            }),
            tap((outcome) => this.showSubmitSummary(outcome))
          );
        }),
        finalize(() => {
          this.isSubmitLoading = false;
        }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe((outcome) => {
        if (outcome?.succeeded?.length || outcome?.schedulesCreated) {
          this.closeTearsheet(true);
        }
      });
  }

  private createSnapshotSchedules(paths: string[]): Observable<number> {
    if (!this.scheduleStep || !paths.length) {
      return of(0);
    }

    const uniquePaths = [...new Set(paths.filter(Boolean))];
    if (!uniquePaths.length) {
      return of(0);
    }

    let schedulesCreated = 0;

    return from(uniquePaths).pipe(
      concatMap((path) =>
        this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('cephfs/snapshot/schedule/' + URLVerbs.CREATE, { path }),
            call: this.snapshotScheduleService.create(this.scheduleStep.buildCreatePayload(path))
          })
          .pipe(
            tap(() => {
              schedulesCreated++;
            }),
            catchError((error) => {
              const detail =
                error?.error?.detail ||
                error?.message ||
                $localize`Failed to create snapshot schedule for '${path}'`;
              if (/Found existing schedule/i.test(detail)) {
                return of(undefined);
              }
              const conflictFrequency =
                this.snapshotScheduleService.parseRetentionConflictFrequency(detail);
              if (conflictFrequency && this.scheduleStep) {
                this.scheduleStep.applyRetentionConflictFromDetail(detail);
              }
              this.notificationService.show(
                NotificationType.error,
                $localize`Failed to create snapshot schedule`,
                conflictFrequency
                  ? $localize`A retention policy with the same frequency already exists for this path. Remove the existing policy or choose a different frequency.`
                  : detail
              );
              return of(undefined);
            })
          )
      ),
      toArray(),
      map(() => schedulesCreated)
    );
  }

  onCancel(): void {
    this.closeTearsheet(false);
  }

  private closeTearsheet(reload: boolean): void {
    this.router.navigate([CEPHFS_MIRRORING_URL, { outlets: { modal: null } }], {
      state: reload ? { reload: true } : undefined
    });
  }

  private showSubmitSummary(outcome: PathSubmitOutput & { schedulesCreated?: number }): void {
    const { failed, skippedByServer, succeeded, schedulesCreated = 0 } = outcome;
    const serverOnlySkipped = skippedByServer.filter(
      (path) => !outcome.alreadyMirrored.includes(path)
    );

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

    if (schedulesCreated && !succeeded.length) {
      this.notificationService.show(
        NotificationType.success,
        schedulesCreated === 1
          ? $localize`Snapshot schedule added for mirrored path on ${this.fsName}`
          : $localize`Snapshot schedules added for ${schedulesCreated} mirrored paths on ${this.fsName}`
      );
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
