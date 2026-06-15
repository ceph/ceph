import { Component } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';
import { RepeatFrequency } from '~/app/shared/enum/repeat-frequency.enum';
import { RetentionFrequency } from '~/app/shared/enum/retention-frequency.enum';

@Component({
  selector: 'cd-mirroring-schedule-step',
  templateUrl: './mirroring-schedule-step.component.html',
  styleUrls: ['./mirroring-schedule-step.component.scss'],
  standalone: false
})
export class MirroringScheduleStepComponent implements TearsheetStep {
  formGroup = new CdFormGroup({
    scheduleName: new UntypedFormControl(''),
    scheduleType: new UntypedFormControl('interval'),
    repeatInterval: new UntypedFormControl(2, [Validators.min(1)]),
    repeatFrequency: new UntypedFormControl(RepeatFrequency.Hourly),
    cronExpression: new UntypedFormControl(''),
    startDate: new UntypedFormControl(''),
    retentionInterval: new UntypedFormControl(7, [Validators.min(1)]),
    retentionFrequency: new UntypedFormControl(RetentionFrequency.Daily),
    retentionCount: new UntypedFormControl(5, [Validators.min(1)])
  });

  scheduleType: 'interval' | 'cron' = 'interval';
  repeatFrequencies = Object.entries(RepeatFrequency);
  retentionFrequencies = Object.entries(RetentionFrequency);

  onScheduleTypeChange(type: 'interval' | 'cron'): void {
    this.scheduleType = type;
    this.formGroup.get('scheduleType').setValue(type);
  }
}
