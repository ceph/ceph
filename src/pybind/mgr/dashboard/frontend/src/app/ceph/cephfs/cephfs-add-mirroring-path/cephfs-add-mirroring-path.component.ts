import { Component, inject, OnInit, ViewChild } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Step } from 'carbon-components-angular';
import { from, Observable, of } from 'rxjs';
import { catchError, concatMap, finalize, map, switchMap, tap, toArray } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import { CEPHFS_MIRRORING_URL } from '~/app/shared/constants/cephfs.constant';
import { CephfsSnapshotScheduleService } from '~/app/shared/api/cephfs-snapshot-schedule.service';
import { TearsheetComponent } from '~/app/shared/components/tearsheet/tearsheet.component';
import { NotificationType } from '~/app/shared/enum/notification-type.enum';
import { NotificationService } from '~/app/shared/services/notification.service';
import { MirroringPathUtils } from './mirroring-path-utils';
import { MirroringPathSelection } from './mirroring-path.model';
import { MirroringPathsStepComponent } from './mirroring-paths-step/mirroring-paths-step.component';
import { MirroringScheduleStepComponent } from './mirroring-schedule-step/mirroring-schedule-step.component';

export { MirroringPathsStepComponent } from './mirroring-paths-step/mirroring-paths-step.component';
export { MirroringScheduleStepComponent } from './mirroring-schedule-step/mirroring-schedule-step.component';

@Component({
  selector: 'cd-cephfs-add-mirroring-path',
  templateUrl: './cephfs-add-mirroring-path.component.html',
  styleUrls: ['./cephfs-add-mirroring-path.component.scss'],
  standalone: false
})
export class CephfsAddMirroringPathComponent implements OnInit {
  @ViewChild(TearsheetComponent) tearsheet!: TearsheetComponent;

  private route = inject(ActivatedRoute);
  private router = inject(Router);
  private cephfsService = inject(CephfsService);
  private snapScheduleService = inject(CephfsSnapshotScheduleService);
  private notificationService = inject(NotificationService);

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

  private cachedToAdd: string[] = [];
  private cachedAlreadyMirrored: string[] = [];
  private cachedPathSelections: MirroringPathSelection[] = [];

  ngOnInit(): void {
    this.fsId = Number(this.route.snapshot.paramMap.get('fsId'));
    const fsName = this.route.snapshot.paramMap.get('fsName') ?? '';
    try {
      this.fsName = decodeURIComponent(fsName);
    } catch {
      this.fsName = fsName;
    }
  }

  onStepChanged(): void {
    this.cachePathsStepState();
    this.syncPathsStepValidity();
  }

  onSubmit(_payload?: unknown): void {
    this.cachePathsStepState();
    this.syncPathsStepValidity();

    const scheduleStep = this.getScheduleStep();
    if (!scheduleStep?.formGroup) {
      return;
    }

    scheduleStep.formGroup.markAllAsTouched();
    scheduleStep.formGroup.updateValueAndValidity();
    if (scheduleStep.formGroup.invalid) {
      return;
    }

    const { toAdd, alreadyMirrored } = this.getSubmitPathState();
    const pathsToProcess = [...toAdd, ...alreadyMirrored];

    if (!pathsToProcess.length) {
      this.notificationService.show(
        NotificationType.error,
        $localize`Select at least one path to continue.`
      );
      return;
    }

    this.isSubmitLoading = true;
    const skippedByServer: string[] = [];
    const pathSelections = this.cachedPathSelections;

    from(pathsToProcess)
      .pipe(
        concatMap((path) =>
          alreadyMirrored.includes(path)
            ? this.addScheduleForMirroredPath(path, scheduleStep, pathSelections)
            : this.addMirroringPath(path, scheduleStep, pathSelections, skippedByServer)
        ),
        toArray(),
        finalize(() => {
          this.isSubmitLoading = false;
          const serverOnlySkipped = skippedByServer.filter(
            (path) => !alreadyMirrored.includes(path)
          );
          if (serverOnlySkipped.length) {
            this.showSkippedPathsWarning(serverOnlySkipped.length);
          }
        })
      )
      .subscribe((results) => {
        if (results.some((path) => !!path)) {
          this.closeTearsheet(true);
        }
      });
  }

  onClose(): void {
    this.closeTearsheet(false);
  }

  private cachePathsStepState(): void {
    const pathsStep = this.getPathsStep();
    if (!pathsStep) {
      return;
    }
    const { toAdd, alreadyMirrored } = pathsStep.getSubmitPaths();
    this.cachedToAdd = toAdd;
    this.cachedAlreadyMirrored = alreadyMirrored;
    this.cachedPathSelections = pathsStep.getPathSelections();
  }

  private getSubmitPathState(): { toAdd: string[]; alreadyMirrored: string[] } {
    const pathsStep = this.getPathsStep();
    if (pathsStep) {
      return pathsStep.getSubmitPaths();
    }
    return { toAdd: this.cachedToAdd, alreadyMirrored: this.cachedAlreadyMirrored };
  }

  private getPathsStep(): MirroringPathsStepComponent | undefined {
    return this.tearsheet?.stepContents?.first?.stepComponent as MirroringPathsStepComponent;
  }

  private getScheduleStep(): MirroringScheduleStepComponent | undefined {
    return this.tearsheet?.stepContents?.toArray()[1]
      ?.stepComponent as MirroringScheduleStepComponent;
  }

  private syncPathsStepValidity(): void {
    const hasSelection = this.cachedToAdd.length + this.cachedAlreadyMirrored.length > 0;
    this.steps = this.steps.map((step, index) =>
      index === 0 ? { ...step, invalid: !hasSelection } : step
    );
  }

  private addScheduleForMirroredPath(
    path: string,
    scheduleStep: MirroringScheduleStepComponent,
    pathSelections: MirroringPathSelection[]
  ): Observable<string | null> {
    const selection = MirroringPathUtils.findPathSelection(path, pathSelections);
    const schedulePayload = scheduleStep.buildCreatePayload(this.fsName, selection);

    return this.snapScheduleService.create(schedulePayload).pipe(
      map(() => path),
      tap(() => {
        this.notificationService.show(
          NotificationType.success,
          $localize`Snapshot schedule added for mirrored path '${path}' on ${this.fsName}`
        );
      }),
      catchError((error) => {
        this.notificationService.show(
          NotificationType.error,
          this.getErrorDetail(error, $localize`Failed to create snapshot schedule for '${path}'`)
        );
        return of(null);
      })
    );
  }

  private addMirroringPath(
    path: string,
    scheduleStep: MirroringScheduleStepComponent,
    pathSelections: MirroringPathSelection[],
    skippedByServer: string[]
  ): Observable<string | null> {
    const selection = MirroringPathUtils.findPathSelection(path, pathSelections);
    const schedulePayload = scheduleStep.buildCreatePayload(this.fsName, selection);
    const pathsStep = this.getPathsStep();

    return this.cephfsService.addMirrorDirectory(this.fsName, path).pipe(
      switchMap(() =>
        this.snapScheduleService.create(schedulePayload).pipe(
          map(() => path),
          catchError((error) => {
            this.notificationService.show(
              NotificationType.error,
              this.getErrorDetail(
                error,
                $localize`Failed to create snapshot schedule for '${path}'`
              )
            );
            return of(path);
          })
        )
      ),
      tap(() => {
        pathsStep?.addTrackedPath(path);
        this.notificationService.show(
          NotificationType.success,
          $localize`Mirroring path '${path}' and schedule added to ${this.fsName}`
        );
      }),
      catchError((error) => {
        const detail = this.getErrorDetail(
          error,
          $localize`Failed to add mirroring path '${path}'`
        );
        if (MirroringPathUtils.isAlreadyTrackedMirrorError(detail)) {
          pathsStep?.addTrackedPath(path);
          skippedByServer.push(path);
          return of(null);
        }
        this.notificationService.show(NotificationType.error, detail);
        return of(null);
      })
    );
  }

  private showSkippedPathsWarning(count: number): void {
    this.notificationService.show(
      NotificationType.warning,
      $localize`Skipped ${count} path(s) that are already mirrored.`
    );
  }

  private getErrorDetail(error: unknown, fallback: string): string {
    const err = error as { error?: { detail?: string }; message?: string };
    return err?.error?.detail || err?.message || fallback;
  }

  private closeTearsheet(reload: boolean): void {
    this.router.navigate([CEPHFS_MIRRORING_URL, { outlets: { modal: null } }], {
      state: reload ? { reload: true } : undefined
    });
  }
}
