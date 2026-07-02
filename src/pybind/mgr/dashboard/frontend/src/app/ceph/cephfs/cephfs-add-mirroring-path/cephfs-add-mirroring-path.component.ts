import { Component, DestroyRef, inject, OnInit, ViewChild } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';
import { Step } from 'carbon-components-angular';
import { from, of } from 'rxjs';
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
import {
  RepeaFrequencyPlural,
  RepeaFrequencySingular
} from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequency } from '~/app/shared/enum/retention-frequency.enum';

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
  reviewSelectedPaths: string[] = [];
  reviewTotalPaths = 0;
  reviewSnapshotInterval = '—';
  reviewRetention = '—';

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
    if (event.current >= 1 && event.current < 2) {
      this.capturePathsReview();
    }
    if (event.current === 2) {
      this.captureScheduleReview();
    }
  }

  private capturePathsReview(): void {
    const { toAdd } = this.pathsStep?.getSubmitPaths() ?? { toAdd: [] };
    this.reviewSelectedPaths = [...toAdd];
    this.reviewTotalPaths = toAdd.length;
  }

  private captureScheduleReview(): void {
    this.capturePathsReview();
    this.reviewSnapshotInterval = this.formatSnapshotInterval();
    this.reviewRetention = this.formatRetention();
  }

  private formatSnapshotInterval(): string {
    const form = this.scheduleStep?.snapScheduleForm;
    if (!form) {
      return '—';
    }
    const interval = form.get('repeatInterval')?.value;
    const frequency = form.get('repeatFrequency')?.value;
    if (!interval || !frequency) {
      return '—';
    }
    const freqLabel =
      interval === 1
        ? RepeaFrequencySingular[frequency] || frequency
        : RepeaFrequencyPlural[frequency] || frequency;
    return `${interval} ${freqLabel}`;
  }

  private formatRetention(): string {
    const policies = this.scheduleStep?.retentionPolicies?.controls;
    if (!policies?.length) {
      return '—';
    }
    const formatted = policies
      .map((control) => {
        const interval = control.get('retentionInterval')?.value;
        const frequency = control.get('retentionFrequency')?.value;
        if (!interval || !frequency) {
          return null;
        }
        const freqLabel =
          Object.entries(RetentionFrequency).find(([, value]) => value === frequency)?.[0] ||
          frequency;
        return `${interval} ${freqLabel}`;
      })
      .filter(Boolean);
    return formatted.length ? formatted.join(', ') : '—';
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

          if (!toAdd.length) {
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
              succeeded: []
            });
          }

          const skippedByServer: string[] = [];
          const failed: PathSubmitFailure[] = [];

          return from(toAdd).pipe(
            concatMap((path) => {
              if (!pathsStep.getSubmitPaths().toAdd.includes(path)) {
                skippedByServer.push(path);
                return of(null);
              }

              return this.cephfsService.addMirrorDirectory(this.fsName, path).pipe(
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
              );
            }),
            toArray(),
            switchMap((results) => {
              const succeeded = results.filter((path): path is string => !!path);
              return this.createSnapshotSchedules(succeeded).pipe(
                map(() => ({ failed, alreadyMirrored, skippedByServer, succeeded }))
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
        if (outcome?.succeeded?.length) {
          this.closeTearsheet(true);
        }
      });
  }

  private createSnapshotSchedules(paths: string[]) {
    if (!this.scheduleStep || !paths.length) {
      return of(undefined);
    }

    return from(paths).pipe(
      concatMap((path) =>
        this.taskWrapper
          .wrapTaskAroundCall({
            task: new FinishedTask('cephfs/snapshot/schedule/' + URLVerbs.CREATE, { path }),
            call: this.snapshotScheduleService.create(this.scheduleStep.buildCreatePayload(path))
          })
          .pipe(
            catchError((error) => {
              const detail =
                error?.error?.detail ||
                error?.message ||
                $localize`Failed to create snapshot schedule for '${path}'`;
              this.notificationService.show(
                NotificationType.error,
                $localize`Failed to create snapshot schedule`,
                detail
              );
              return of(undefined);
            })
          )
      ),
      toArray(),
      map(() => undefined)
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

  private showSubmitSummary(outcome: PathSubmitOutput): void {
    const { failed, alreadyMirrored, skippedByServer, succeeded } = outcome;
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
