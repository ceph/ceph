import { Component, Input, OnInit } from '@angular/core';

import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

@Component({
  selector: 'cd-mirroring-review-step',
  templateUrl: './mirroring-review-step.component.html',
  styleUrls: ['./mirroring-review-step.component.scss'],
  standalone: false
})
export class MirroringReviewStepComponent implements OnInit, TearsheetStep {
  @Input() destinationCluster = '—';
  @Input() destinationFilesystem = '—';
  @Input() totalPaths = 0;
  @Input() snapshotInterval = '—';
  @Input() retention = '—';
  @Input() existingScheduleCount = 0;

  formGroup!: CdFormGroup;

  ngOnInit(): void {
    this.formGroup = new CdFormGroup({});
  }
}