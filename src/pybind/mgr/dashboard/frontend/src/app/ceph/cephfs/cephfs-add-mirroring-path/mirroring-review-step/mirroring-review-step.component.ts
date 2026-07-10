import { Component, DestroyRef, inject, Input, OnInit } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { of } from 'rxjs';
import { catchError } from 'rxjs/operators';

import { CephfsService } from '~/app/shared/api/cephfs.service';
import {
  RepeaFrequencyPlural,
  RepeaFrequencySingular
} from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequencyCopy } from '~/app/shared/enum/retention-frequency.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { Daemon, Filesystem } from '~/app/shared/models/cephfs.model';
import { SnapshotScheduleFormValue } from '~/app/shared/models/snapshot-schedule';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { CephfsSnapshotscheduleFormComponent } from '../../cephfs-snapshotschedule-form/cephfs-snapshotschedule-form.component';
import { MirroringPathsStepComponent } from '../mirroring-paths-step/mirroring-paths-step.component';

@Component({
  selector: 'cd-mirroring-review-step',
  templateUrl: './mirroring-review-step.component.html',
  styleUrls: ['./mirroring-review-step.component.scss'],
  standalone: false
})
export class MirroringReviewStepComponent implements OnInit, TearsheetStep {
  @Input() fsName = '';
  @Input() pathsStep?: MirroringPathsStepComponent;
  @Input() scheduleStep?: CephfsSnapshotscheduleFormComponent;

  destinationCluster = '—';
  destinationFilesystem = '—';

  formGroup!: CdFormGroup;

  private cephfsService = inject(CephfsService);
  private destroyRef = inject(DestroyRef);

  ngOnInit(): void {
    this.formGroup = new CdFormGroup({});
    this.loadDestinationInfo();
  }

  get totalPaths(): number {
    return this.pathsStep?.getSubmitPaths()?.toAdd?.length ?? 0;
  }

  get snapshotInterval(): string {
    const values = this.getScheduleValues();
    if (values?.repeatInterval == null || !values?.repeatFrequency) {
      return '—';
    }

    const unit =
      values.repeatInterval === 1
        ? RepeaFrequencySingular[values.repeatFrequency]
        : RepeaFrequencyPlural[values.repeatFrequency];
    if (!unit) {
      return '—';
    }

    return $localize`Every ${values.repeatInterval} ${unit}`;
  }

  get retention(): string {
    const policies = this.getScheduleValues()?.retentionPolicies;
    if (!policies?.length) {
      return '—';
    }

    const formatted = policies
      .filter((policy) => policy?.retentionInterval != null && policy?.retentionFrequency != null)
      .map((policy) => {
        const frequency =
          RetentionFrequencyCopy[policy.retentionFrequency] ?? policy.retentionFrequency;
        return `${policy.retentionInterval} ${frequency}`;
      });

    return formatted.length ? formatted.join(', ') : '—';
  }

  get existingScheduleCount(): number {
    return 0;
  }

  private getScheduleValues(): SnapshotScheduleFormValue | undefined {
    return this.scheduleStep?.snapScheduleForm?.getRawValue() as
      | SnapshotScheduleFormValue
      | undefined;
  }

  private loadDestinationInfo(): void {
    if (!this.fsName) {
      return;
    }

    this.cephfsService
      .listDaemonStatus()
      .pipe(catchError(() => of([] as Daemon[])), takeUntilDestroyed(this.destroyRef))
      .subscribe((daemons: Daemon[]) => {
        for (const daemon of daemons) {
          const filesystem = daemon.filesystems?.find((fs: Filesystem) => fs.name === this.fsName);
          if (!filesystem) {
            continue;
          }

          const peer = filesystem.peers?.[0];
          this.destinationCluster = peer?.remote?.cluster_name ?? '—';
          this.destinationFilesystem = peer?.remote?.fs_name ?? '—';
          return;
        }
      });
  }
}
