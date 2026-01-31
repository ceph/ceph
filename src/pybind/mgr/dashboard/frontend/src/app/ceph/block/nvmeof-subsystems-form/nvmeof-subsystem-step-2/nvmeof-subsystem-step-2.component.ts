import { Component, Input, OnInit } from '@angular/core';
import { UntypedFormControl, Validators } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';

const Authentication = {
  Unidirectional: 'unidirectional',
  Bidirectional: 'bidirectional'
};

@Component({
  selector: 'cd-nvmeof-subsystem-step-two',
  templateUrl: './nvmeof-subsystem-step-2.component.html',
  styleUrls: ['./nvmeof-subsystem-step-2.component.scss'],
  standalone: false
})
export class NvmeofSubsystemsStepTwoComponent implements OnInit, TearsheetStep {
  @Input() group!: string;
  formGroup: CdFormGroup;
  action: string;
  pageURL: string;
  INVALID_TEXTS = {
    required: $localize`This field is required`
  };
  AUTHENTICATION = Authentication;
  uniHelperText = $localize`Each host can provide an optional DH-HMAC-CHAP key. The subsystem does not require its own key.`;
  biHelperText = $localize`Both subsystem and hosts must provide DH-HMAC-CHAP keys. All connections will be verified in both directions.`;

  constructor(public actionLabels: ActionLabelsI18n, public activeModal: NgbActiveModal) {}

  ngOnInit() {
    this.createForm();
  }

  createForm() {
    this.formGroup = new CdFormGroup({
      authType: new UntypedFormControl(Authentication.Unidirectional, {
        validators: [Validators.required]
      }),
      subsystemDchapKey: new UntypedFormControl('', {
        validators: [Validators.required]
      })
    });
  }
}
