import { Component, Input, OnInit } from '@angular/core';
import { NgForm, ValidatorFn, Validators } from '@angular/forms';

import { CdFormGroup } from '~/app/shared/forms/cd-form-group';

@Component({
  selector: 'cd-iscsi-setting',
  templateUrl: './iscsi-setting.component.html',
  styleUrls: ['./iscsi-setting.component.scss']
})
export class IscsiSettingComponent implements OnInit {
  @Input()
  settingsForm: CdFormGroup;
  @Input()
  formDir: NgForm;
  @Input()
  setting: string;
  @Input()
  limits: object;

  ngOnInit() {
    const validators: ValidatorFn[] = [];
    if ('min' in this.limits) {
      validators.push(Validators.min(Number(this.limits['min'])));
    }
    if ('max' in this.limits) {
      validators.push(Validators.max(Number(this.limits['max'])));
    }
    this.settingsForm.get(this.setting).setValidators(validators);
  }
}
