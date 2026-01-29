import { Component, Input, OnInit } from '@angular/core';
import { FormArray, UntypedFormControl } from '@angular/forms';
import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';

import { ActionLabelsI18n } from '~/app/shared/constants/app.constants';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { AUTHENTICATION } from '~/app/shared/models/nvmeof';
import { TearsheetStep } from '~/app/shared/models/tearsheet-step';

@Component({
  selector: 'cd-nvmeof-subsystem-step-three',
  templateUrl: './nvmeof-subsystem-step-3.component.html',
  styleUrls: ['./nvmeof-subsystem-step-3.component.scss'],
  standalone: false
})
export class NvmeofSubsystemsStepThreeComponent implements OnInit, TearsheetStep {
  @Input() group!: string;
  formGroup: CdFormGroup;
  action: string;
  pageURL: string;
  INVALID_TEXTS = {
    required: $localize`This field is required`
  };
  AUTHENTICATION = AUTHENTICATION;

  constructor(public actionLabels: ActionLabelsI18n, public activeModal: NgbActiveModal) {}

  ngOnInit() {
    this.createForm();
  }

  createForm() {
    this.formGroup = new CdFormGroup({
      authType: new UntypedFormControl(AUTHENTICATION.Unidirectional),
      subsystemDchapKey: new UntypedFormControl(null),
      hostDchapKeyList: new FormArray([this.createHostDchapKeyItem()])
    });
  }

  createHostDchapKeyItem() {
    return new CdFormGroup({
      key: new UntypedFormControl(null),
      hostNQN: new UntypedFormControl('')
    });
  }

  get hostDchapKeyList() {
    return this.formGroup.get('hostDchapKeyList') as FormArray;
  }
}
