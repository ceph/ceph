import { Component, DestroyRef, inject, OnInit, ViewChild } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { ActivatedRoute, Router } from '@angular/router';
import { Step } from 'carbon-components-angular';
import { from, of } from 'rxjs';
import { catchError, concatMap, finalize, map, switchMap, tap, toArray } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CEPHFS_MIRRORING_URL } from '~/app/shared/constants/cephfs.constant';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { RepeaFrequencySingular, RepeaFrequencyPlural } from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequencyCopy } from '~/app/shared/enum/retention-frequency.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { MirroringPathUtils } from './mirroring-path-utils';
import { PathSubmitFailure, PathSubmitOutput } from './mirroring-path.model';
import { MirroringPathsStepComponent } from './mirroring-paths-step/mirroring-paths-step.component';
import { MirroringScheduleStepComponent } from './mirroring-schedule-step/mirroring-schedule-step.component';

@Component({
  selector: 'cd-cephfs-add-mirroring-path',
  templateUrl: './cephfs-add-mirroring-path.component.html',
  styleUrls: ['./cephfs-add-mirroring-path.component.scss'],
  standalone: false
})
export class CephfsAddMirroringPathComponent implements OnInit {
  @ViewChild('pathsStep') pathsStep!: MirroringPathsStepComponent;
  @ViewChild('scheduleStep') scheduleStep!: MirroringScheduleStepComponent;

  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private cephfsService = inject(CephfsService);
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

  reviewDestinationCluster = '—';
  reviewDestinationFs = '—';
  reviewTotalPaths = 0;
  reviewSnapshotInterval = '—';
  reviewRetention = '—';
  reviewExistingScheduleCount = 0;

  ngOnInit(): void {
    this.fsId = Number(this.route.snapshot.paramMap.get('fsId'));
    const fsName = this.route.snapshot.paramMap.get('fsName') ?? '';
    try {
      this.fsName = decodeURIComponent(fsName);
    } catch {
      this.fsName = fsName;
    }
    this.loadDestinationInfo();
  }

  onStepChanged(event: { current: number }): void {
    if (event.current === 2) {
      this.populateReviewData();
    }
  }

  onSubmit(): void {
    const pathsStep = this.pathsStep;
    if (!pathsStep?.formGroup) {
      return;
    }

    pathsStep.formGroup.markAllAsTouched();
    pathsStep.formGroup.updateValueAndValidity();
    if (pathsStep.formGroup.invalid) {
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
            return of([] as (string | null)[]);
          }

          const skippedByServer: string[] = [];
          const failed: PathSubmitFailure[] = [];

          return from(toAdd).pipe(
            concatMap((path) => {
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
            map((results) => {
              const succeeded = results.filter((p): p is string => !!p);
              this.showSubmitSummary({ failed, alreadyMirrored, skippedByServer, succeeded });
              return results;
            })
          );
        }),
        finalize(() => {
          this.isSubmitLoading = false;
        }),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe((results) => {
        if (results.some((path) => !!path)) {
          this.closeTearsheet(true);
        }
      });
  }

  onCancel(): void {
    this.closeTearsheet(false);
  }

  private populateReviewData(): void {
    const { toAdd, alreadyMirrored } = this.pathsStep?.getSubmitPaths() ?? {
      toAdd: [],
      alreadyMirrored: []
    };
    this.reviewTotalPaths = toAdd.length + alreadyMirrored.length;

    const scheduleValues = this.scheduleStep?.formGroup?.getRawValue();
    if (scheduleValues) {
      this.reviewSnapshotInterval = this.formatSnapshotInterval(
        scheduleValues.repeatInterval,
        scheduleValues.repeatFrequency
      );
      this.reviewRetention = this.formatRetention(
        scheduleValues.retentionInterval,
        scheduleValues.retentionFrequency,
        scheduleValues.retentionCount
      );
    }
  }

  private formatSnapshotInterval(interval: number, frequency: string): string {
    if (!interval || !frequency) return '—';
    const freqLabel =
      interval === 1
        ? (RepeaFrequencySingular as Record<string, string>)[frequency] ?? frequency
        : (RepeaFrequencyPlural as Record<string, string>)[frequency] ?? frequency;
    return interval === 1
      ? $localize`Every ${freqLabel}`
      : $localize`Every ${interval} ${freqLabel}`;
  }

  private formatRetention(interval: number, frequency: string, count?: number): string {
    if (!interval || !frequency) return '—';
    const freqLabel =
      (RetentionFrequencyCopy as Record<string, string>)[frequency]?.toLowerCase() ?? frequency;
    let label = `${interval} ${freqLabel}`;
    if (count) {
      label += $localize`, last ${count} copies`;
    }
    return label;
  }

  private loadDestinationInfo(): void {
    this.cephfsService
      .listDaemonStatus()
      .pipe(
        catchError(() => of([])),
        takeUntilDestroyed(this.destroyRef)
      )
      .subscribe((daemons) => {
        for (const daemon of daemons ?? []) {
          const fs = daemon.filesystems?.find(
            (f: any) => f.name === this.fsName || f.filesystem_id === this.fsId
          );
          if (fs?.peers?.length) {
            this.reviewDestinationCluster = fs.peers[0].remote?.cluster_name ?? '—';
            this.reviewDestinationFs = fs.peers[0].remote?.fs_name ?? '—';
            return;
          }
        }
      });
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
