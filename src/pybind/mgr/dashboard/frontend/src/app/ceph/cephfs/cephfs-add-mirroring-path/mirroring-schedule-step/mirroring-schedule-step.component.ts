import { Component, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { RepeatFrequency } from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequency } from '~/app/shared/enum/retention-frequency.enum';
import {
  MIRRORING_SCHEDULE_TIMEZONES,
  SCHEDULE_TYPE,
  START_AM_PM,
  StartAmPm
} from './mirroring-schedule-step.model';

@Component({
  selector: 'cd-mirroring-schedule-step',
  templateUrl: './mirroring-schedule-step.component.html',
  standalone: false
})
export class MirroringScheduleStepComponent implements OnInit, TearsheetStep {
  readonly SCHEDULE_TYPE = SCHEDULE_TYPE;
  readonly START_AM_PM = START_AM_PM;
  readonly timezones = MIRRORING_SCHEDULE_TIMEZONES;

  formGroup: CdFormGroup;

  repeatFrequencies = Object.entries(RepeatFrequency);
  retentionFrequencies = Object.entries(RetentionFrequency);

  startDatePickerValue: string[] = [];
  startTimeValue = '';
  startAmPmValue: StartAmPm = START_AM_PM.AM;

  ngOnInit(): void {
    this.createForm();
  }

  createForm(): void {
    this.formGroup = new CdFormGroup({
      scheduleName: new UntypedFormControl(''),
      scheduleType: new UntypedFormControl(SCHEDULE_TYPE.INTERVAL),
      repeatInterval: new UntypedFormControl(2, [Validators.min(1)]),
      repeatFrequency: new UntypedFormControl(RepeatFrequency.Hourly),
      startDate: new UntypedFormControl(''),
      startTime: new UntypedFormControl(''),
      startAmPm: new UntypedFormControl(START_AM_PM.AM),
      startTimezone: new UntypedFormControl(''),
      retentionInterval: new UntypedFormControl(7, [Validators.min(1)]),
      retentionFrequency: new UntypedFormControl(RetentionFrequency.Daily),
      retentionCount: new UntypedFormControl(5, [Validators.min(1)])
    });
  }

  onStartDateChange(dates: string[]): void {
    this.startDatePickerValue = dates ?? [];
    const value = dates?.length ? dates[0] : '';
    this.formGroup.controls.startDate.setValue(value);
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
}
