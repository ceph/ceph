import { Component, EventEmitter, Output } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';

import { BsModalRef } from 'ngx-bootstrap/modal';

import { CdFormBuilder } from '../../../../shared/forms/cd-form-builder';
import { CdFormGroup } from '../../../../shared/forms/cd-form-group';
import { PrometheusSilenceMatcher } from '../../../../shared/models/prometheus-silence';

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

  form: CdFormGroup;

  constructor(private formBuilder: CdFormBuilder, public bsModalRef: BsModalRef) {
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

  checkPrometheusPromQlResults() {
    // "node_load1 > 1 and node_load1 > 3"
    // http://albatros-latitude-7480:9090/graph?g0.range_input=1h&g0.expr=node_load1%20%3E%201%20and%20node_load1%20%3E%203&g0.tab=1
  }

  generatePromQlURL(host: string, promQl: string) {
    return host + '/api/v1/query?query=' + encodeURI(promQl);
  }

  onSubmit() {
    this.submitAction.emit(this.form.value);
    this.bsModalRef.hide();
  }
}
