import { Component, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';

import { RepeatFrequency } from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequency } from '~/app/shared/enum/retention-frequency.enum';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import {
  buildSnapshotScheduleCreatePayload,
  RetentionPolicy,
  SnapshotScheduleCreatePayload
} from '~/app/shared/models/snapshot-schedule';
import { MirroringPathSelection } from '../mirroring-path.model';

@Component({
  selector: 'cd-mirroring-schedule-step',
  templateUrl: './mirroring-schedule-step.component.html',
  styleUrls: ['./mirroring-schedule-step.component.scss'],
  standalone: false
})
export class MirroringScheduleStepComponent implements OnInit, TearsheetStep {
  formGroup!: CdFormGroup;

  repeatFrequencies = Object.entries(RepeatFrequency);
  retentionFrequencies = Object.entries(RetentionFrequency);

  ngOnInit(): void {
    this.formGroup = this.buildForm();
  }

  buildCreatePayload(
    fsName: string,
    selection: MirroringPathSelection
  ): SnapshotScheduleCreatePayload {
    const values = this.formGroup.getRawValue();
    const retentionPolicies = this.buildRetentionPolicies(values);

    return buildSnapshotScheduleCreatePayload({
      fs: fsName,
      path: selection.path,
      repeatInterval: values.repeatInterval,
      repeatFrequency: values.repeatFrequency,
      startDate: values.startDate,
      retentionPolicies,
      subvol: selection.subvol,
      group: selection.group
    });
  }

  private buildForm(): CdFormGroup {
    return new CdFormGroup({
      repeatInterval: new FormControl(1, [Validators.min(1)]),
      repeatFrequency: new FormControl(RepeatFrequency.Daily),
      startDate: new FormControl(''),
      retentionInterval: new FormControl(7, [Validators.min(1)]),
      retentionFrequency: new FormControl(RetentionFrequency.Daily),
      retentionCount: new FormControl(5, [Validators.min(1)])
    });
  }

  private buildRetentionPolicies(values: {
    retentionInterval: number;
    retentionFrequency: string;
    retentionCount: number;
  }): RetentionPolicy[] {
    const retentionPolicies: RetentionPolicy[] = [];

    if (values.retentionInterval && values.retentionFrequency) {
      retentionPolicies.push({
        retentionInterval: values.retentionInterval,
        retentionFrequency: values.retentionFrequency
      });
    }
    if (values.retentionCount) {
      retentionPolicies.push({
        retentionInterval: values.retentionCount,
        retentionFrequency: 'n'
      });
    }

    return retentionPolicies;
  }
}
