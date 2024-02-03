import { Component, OnInit } from '@angular/core';
import { AsyncValidatorFn, UntypedFormControl, ValidatorFn, Validators } from '@angular/forms';

import { NgbActiveModal } from '@ng-bootstrap/ng-bootstrap';
import _ from 'lodash';

import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdFormModalFieldConfig } from '~/app/shared/models/cd-form-modal-field-config';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';

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
    const controlsConfig: Record<string, UntypedFormControl> = {};
    this.fields.forEach((field) => {
      controlsConfig[field.name] = this.createFormControl(field);
    });
    this.formGroup = this.formBuilder.group(controlsConfig);
  }

  private createFormControl(field: CdFormModalFieldConfig): UntypedFormControl {
    let validators: ValidatorFn[] = [];
    let asyncValidators: AsyncValidatorFn[] = [];
    if (_.isBoolean(field.required) && field.required) {
      validators.push(Validators.required);
    }
    if (field.validators) {
      validators = validators.concat(field.validators);
    }
    if (field.asyncValidators) {
      asyncValidators = asyncValidators.concat(field.asyncValidators);
    }
    return new UntypedFormControl(
      _.defaultTo(
        field.type === 'binary' ? this.dimlessBinaryPipe.transform(field.value) : field.value,
        null
      ),
      { validators, asyncValidators }
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
    if (error === 'pattern') {
      return $localize`Size must be a number or in a valid format. eg: 5 GiB`;
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
