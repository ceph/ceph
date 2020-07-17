import { Component, OnInit } from '@angular/core';
import { FormControl, ValidatorFn, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import * as _ from 'lodash';

import { CdFormBuilder } from '../../forms/cd-form-builder';
import { CdFormGroup } from '../../forms/cd-form-group';
import { CdFormModalFieldConfig } from '../../models/cd-form-modal-field-config';
import { DimlessBinaryPipe } from '../../pipes/dimless-binary.pipe';
import { FormatterService } from '../../services/formatter.service';

@Component({
  selector: 'cd-form-modal',
  templateUrl: './form-modal.component.html',
  styleUrls: ['./form-modal.component.scss']
})
export class FormModalComponent implements OnInit {
  // Input
  titleText: string;
  message: string;
  fields: CdFormModalFieldConfig[];
  submitButtonText: string;
  onSubmit: Function;

  // Internal
  formGroup: CdFormGroup;

  constructor(
    public activeModal: NgbActiveModal,
    private formBuilder: CdFormBuilder,
    private formatter: FormatterService,
    private dimlessBinaryPipe: DimlessBinaryPipe
  ) {}

  ngOnInit() {
    this.createForm();
  }

  createForm() {
    const controlsConfig: Record<string, FormControl> = {};
    this.fields.forEach((field) => {
      controlsConfig[field.name] = this.createFormControl(field);
    });
    this.formGroup = this.formBuilder.group(controlsConfig);
  }

  private createFormControl(field: CdFormModalFieldConfig): FormControl {
    let validators: ValidatorFn[] = [];
    if (_.isBoolean(field.required) && field.required) {
      validators.push(Validators.required);
    }
    if (field.validators) {
      validators = validators.concat(field.validators);
    }
    return new FormControl(
      _.defaultTo(
        field.type === 'binary' ? this.dimlessBinaryPipe.transform(field.value) : field.value,
        null
      ),
      { validators }
    );
  }

  getError(field: CdFormModalFieldConfig): string {
    const formErrors = this.formGroup.get(field.name).errors;
    const errors = Object.keys(formErrors).map((key) => {
      return this.getErrorMessage(key, formErrors[key], field.errors);
    });
    return errors.join('<br>');
  }

  private getErrorMessage(
    error: string,
    errorContext: any,
    fieldErrors: { [error: string]: string }
  ): string {
    if (fieldErrors) {
      const customError = fieldErrors[error];
      if (customError) {
        return customError;
      }
    }
    if (['binaryMin', 'binaryMax'].includes(error)) {
      // binaryMin and binaryMax return a function that take I18n to
      // provide a translated error message.
      return errorContext();
    }
    if (error === 'required') {
      return $localize`This field is required.`;
    }
    return $localize`An error occurred.`;
  }

  onSubmitForm(values: any) {
    const binaries = this.fields
      .filter((field) => field.type === 'binary')
      .map((field) => field.name);
    binaries.forEach((key) => {
      const value = values[key];
      if (value) {
        values[key] = this.formatter.toBytes(value);
      }
    });
    this.activeModal.close();
    if (_.isFunction(this.onSubmit)) {
      this.onSubmit(values);
    }
  }
}
