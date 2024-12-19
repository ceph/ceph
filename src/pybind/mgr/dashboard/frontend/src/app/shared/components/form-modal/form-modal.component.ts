import { Component, Inject, OnInit, Optional } from '@angular/core';
import { AsyncValidatorFn, UntypedFormControl, ValidatorFn, Validators } from '@angular/forms';

import { BaseModal } from 'carbon-components-angular';
import _ from 'lodash';
import { Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged } from 'rxjs/operators';

import { CdFormBuilder } from '~/app/shared/forms/cd-form-builder';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdFormModalFieldConfig } from '~/app/shared/models/cd-form-modal-field-config';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';
import { ComboBoxItem } from '../../models/combo-box.model';

@Component({
  selector: 'cd-form-modal',
  templateUrl: './form-modal.component.html',
  styleUrls: ['./form-modal.component.scss']
})
export class FormModalComponent extends BaseModal implements OnInit {
  // Internal
  formGroup: CdFormGroup;

  searchSubject = new Subject<{ searchString: string; fieldName: string }>();

  constructor(
    private formBuilder: CdFormBuilder,
    private formatter: FormatterService,
    private dimlessBinaryPipe: DimlessBinaryPipe,

    // Inputs
    @Optional() @Inject('titleText') public titleText: string,
    @Optional() @Inject('fields') public fields: CdFormModalFieldConfig[],
    @Optional() @Inject('submitButtonText') public submitButtonText: string,
    @Optional() @Inject('onSubmit') public onSubmit: Function,
    @Optional() @Inject('message') public message = '',
    @Optional() @Inject('updateAsyncValidators') public updateAsyncValidators: Function
  ) {
    super();
  }

  ngOnInit() {
    this.createForm();
    // subscribe to the searchSubject to add new options to the combobox
    this.searchSubject
      .pipe(debounceTime(500), distinctUntilChanged())
      .subscribe(({ searchString, fieldName }) => {
        const field = this.fields.find((f) => f.name === fieldName);
        if (field) {
          // to make sure only unique values exists in the list
          const exists = field.typeConfig.options.some(
            (option: ComboBoxItem) => option.content === searchString
          );
          if (!exists) {
            field.typeConfig.options = field.typeConfig.options.concat({ content: searchString });
          }
        }
      });
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

    const control = new UntypedFormControl(
      _.defaultTo(
        field.type === 'binary' ? this.dimlessBinaryPipe.transform(field.value) : field.value,
        null
      ),
      { validators, asyncValidators }
    );

    if (field.type === 'select-badges' && field.value) control.setValue(field.value);

    if (field.valueChangeListener) {
      control.valueChanges.subscribe((value) => {
        const validatorToUpdate = this.updateAsyncValidators(value);
        this.updateValidation(field.dependsOn, validatorToUpdate);
      });
    }
    return control;
  }

  getError(field: CdFormModalFieldConfig): string {
    const formErrors = this.formGroup.get(field.name).errors;
    if (!formErrors) {
      return '';
    }
    const errors = Object.keys(formErrors)?.map((key) => {
      return this.getErrorMessage(key, formErrors[key], field.errors);
    });
    return errors?.join('<br>');
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
    this.closeModal();
    if (_.isFunction(this.onSubmit)) {
      this.onSubmit(values);
    }
  }

  updateValidation(name?: string, validator?: AsyncValidatorFn[]) {
    const field = this.formGroup.get(name);
    field.setAsyncValidators(validator);
    field.updateValueAndValidity();
  }

  onSearch(searchString: string, fieldName: string) {
    // only add to the list if the search string is more than 1 character
    if (searchString.length > 1) {
      this.searchSubject.next({ searchString, fieldName });
    }
  }
}
