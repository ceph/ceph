import { Component, EventEmitter, Output } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { PrometheusSilenceMatcher } from '../../../../shared/models/prometheus-silence';
import { AlertmanagerAlert, PrometheusRule } from '../../../../shared/models/prometheus-alerts';

@Component({
  selector: 'cd-silence-matcher-modal',
  templateUrl: './silence-matcher-modal.component.html',
  styleUrls: ['./silence-matcher-modal.component.scss']
})
export class SilenceMatcherModalComponent {
  /**
   * The event that is triggered when the 'Add' or 'Update' button
   * has been pressed.
   */
  @Output()
  submitAction = new EventEmitter();

  nameAttributes = ['alertname', 'instance', 'job', 'severity'];
  form: CdFormGroup;

  alerts: AlertmanagerAlert[]; // Will be set by silence form
  rules: PrometheusRule[]; // Will be set by silence form

  constructor(
    private formBuilder: CdFormBuilder,
    public bsModalRef: BsModalRef
  ) {
    this.createForm();
  }

  createForm() {
    this.form = this.formBuilder.group({
      name: [null, [Validators.required]],
      value: [null, [Validators.required]],
      isRegex: new FormControl(false)
    });
  }

  preFillControls(matcher: PrometheusSilenceMatcher) {
    this.form.setValue(matcher);
  }

  onSubmit() {
    this.submitAction.emit(this.form.value);
    this.bsModalRef.hide();
  }
}
