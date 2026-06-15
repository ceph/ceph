import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

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
import {
  MIRRORING_SCHEDULE_TIMEZONES,
  SCHEDULE_TYPE,
  START_AM_PM,
  StartAmPm
} from './mirroring-schedule-step.model';

@Component({
  selector: 'cd-mirroring-schedule-step',
  templateUrl: './mirroring-schedule-step.component.html',
  styleUrls: ['./mirroring-schedule-step.component.scss'],
  standalone: false
})
export class MirroringScheduleStepComponent implements OnInit, TearsheetStep {
  readonly SCHEDULE_TYPE = SCHEDULE_TYPE;
  readonly START_AM_PM = START_AM_PM;
  readonly timezones = MIRRORING_SCHEDULE_TIMEZONES;

  formGroup!: CdFormGroup;
  startDatePickerValue: string[] = [];
  startTimeValue = '';
  startAmPmValue: StartAmPm = START_AM_PM.AM;

  repeatFrequencies = Object.entries(RepeatFrequency);
  retentionFrequencies = Object.entries(RetentionFrequency);

  ngOnInit(): void {
    this.formGroup = this.buildForm();
  }

  onStartDateChange(dates: string[]): void {
    this.startDatePickerValue = dates ?? [];
    this.formGroup.controls.startDate.setValue(dates?.length ? dates[0] : '');
  }

  onStartTimeChange(event?: string): void {
    if (typeof event === 'string') {
      if (Object.values(START_AM_PM).includes(event as StartAmPm)) {
        this.startAmPmValue = event as StartAmPm;
      } else {
        this.startTimeValue = event;
      }
    }
    this.formGroup.controls.startTime.setValue(this.startTimeValue);
    this.formGroup.controls.startAmPm.setValue(this.startAmPmValue);
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
      scheduleName: new UntypedFormControl(''),
      scheduleType: new UntypedFormControl(SCHEDULE_TYPE.INTERVAL),
      repeatInterval: new UntypedFormControl(1, [Validators.min(1)]),
      repeatFrequency: new UntypedFormControl(RepeatFrequency.Daily),
      startDate: new UntypedFormControl(''),
      startTime: new UntypedFormControl(''),
      startAmPm: new UntypedFormControl(START_AM_PM.AM),
      startTimezone: new UntypedFormControl(''),
      retentionInterval: new UntypedFormControl(7, [Validators.min(1)]),
      retentionFrequency: new UntypedFormControl(RetentionFrequency.Daily),
      retentionCount: new UntypedFormControl(5, [Validators.min(1)])
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
