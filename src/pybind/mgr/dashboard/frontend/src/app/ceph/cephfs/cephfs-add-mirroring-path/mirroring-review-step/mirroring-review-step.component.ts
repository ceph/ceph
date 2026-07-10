import { Component, Input, OnInit } from '@angular/core';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
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

  formGroup!: CdFormGroup;

  ngOnInit(): void {
    this.formGroup = new CdFormGroup({});
  }

  get pathsToAdd(): string[] {
    return this.pathsStep?.getSubmitPaths()?.toAdd ?? [];
  }

  get scheduleSummary(): string {
    return this.scheduleStep?.getScheduleSummary() ?? '';
  }
}
