import { Component, Input, OnInit } from '@angular/core';

import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { MirroringPathsStepComponent } from '../mirroring-paths-step/mirroring-paths-step.component';
import { CephfsSnapshotscheduleFormComponent } from '../../cephfs-snapshotschedule-form/cephfs-snapshotschedule-form.component';
import {
  RepeatFrequencyPlural,
  RepeatFrequencySingular
} from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequency } from '~/app/shared/enum/retention-frequency.enum';

@Component({
  selector: 'cd-mirroring-review-step',
  templateUrl: './mirroring-review-step.component.html',
  styleUrls: ['./mirroring-review-step.component.scss'],
  standalone: false
})
export class MirroringReviewStepComponent implements OnInit, TearsheetStep {
  @Input() fsName = '—';
  @Input() pathsStep: MirroringPathsStepComponent;
  @Input() scheduleStep: CephfsSnapshotscheduleFormComponent;

  formGroup!: CdFormGroup;

  get totalPaths(): number {
    return this.pathsStep?.getSubmitPaths()?.toAdd?.length ?? 0;
  }

  get snapshotInterval(): string {
    if (!this.scheduleStep?.snapScheduleForm) {
      return '—';
    }
    const interval = this.scheduleStep.snapScheduleForm.get('repeatInterval')?.value;
    const frequency = this.scheduleStep.snapScheduleForm.get('repeatFrequency')?.value;
    if (!interval || !frequency) {
      return '—';
    }
    const freqLabel =
      interval === 1
        ? RepeatFrequencySingular[frequency] || frequency
        : RepeatFrequencyPlural[frequency] || frequency;
    return `${interval} ${freqLabel}`;
  }

  get retention(): string {
    if (!this.scheduleStep?.retentionPolicies?.controls?.length) {
      return '—';
    }
    const policies = this.scheduleStep.retentionPolicies.controls
      .map((control) => {
        const interval = control.get('retentionInterval')?.value;
        const frequency = control.get('retentionFrequency')?.value;
        if (!interval || !frequency) {
          return null;
        }
        const freqLabel =
          Object.entries(RetentionFrequency).find(([, v]) => v === frequency)?.[0] || frequency;
        return `${interval} ${freqLabel}`;
      })
      .filter(Boolean);
    return policies.length ? policies.join(', ') : '—';
  }

  ngOnInit(): void {
    this.formGroup = new CdFormGroup({});
  }
}
