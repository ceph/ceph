import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';

import * as _ from 'lodash';
import { BsModalRef } from 'ngx-bootstrap/modal';

import { CdFormBuilder } from '../../forms/cd-form-builder';

interface CdFormFieldConfig {
  type: 'textInput';
  name: string;
  label?: string;
  value?: any;
  required?: boolean;
}

@Component({
  selector: 'cd-form-modal',
  templateUrl: './form-modal.component.html',
  styleUrls: ['./form-modal.component.scss']
})
export class FormModalComponent implements OnInit {
  // Input
  titleText: string;
  message: string;
  fields: CdFormFieldConfig[];
  submitButtonText: string;
  onSubmit: Function;

  // Internal
  formGroup: FormGroup;

  constructor(public bsModalRef: BsModalRef, private formBuilder: CdFormBuilder) {}

  createForm() {
    const controlsConfig = {};
    this.fields.forEach((field) => {
      const validators = [];
      if (_.isBoolean(field.required) && field.required) {
        validators.push(Validators.required);
      }
      controlsConfig[field.name] = new FormControl(_.defaultTo(field.value, null), { validators });
    });
    this.formGroup = this.formBuilder.group(controlsConfig);
  }

  ngOnInit() {
    this.createForm();
  }

  onSubmitForm(values) {
    this.bsModalRef.hide();
    if (_.isFunction(this.onSubmit)) {
      this.onSubmit(values);
    }
  }
}
