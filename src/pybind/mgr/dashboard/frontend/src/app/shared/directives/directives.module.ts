import { NgModule } from '@angular/core';

import { AuthStorageDirective } from './auth-storage.directive';
import { AutofocusDirective } from './autofocus.directive';
import { DimlessBinaryPerSecondDirective } from './dimless-binary-per-second.directive';
import { DimlessBinaryDirective } from './dimless-binary.directive';
import { FormInputDisableDirective } from './form-input-disable.directive';
import { FormLoadingDirective } from './form-loading.directive';
import { FormScopeDirective } from './form-scope.directive';
import { IopsDirective } from './iops.directive';
import { MillisecondsDirective } from './milliseconds.directive';
import { CdFormControlDirective } from './ng-bootstrap-form-validation/cd-form-control.directive';
import { CdFormGroupDirective } from './ng-bootstrap-form-validation/cd-form-group.directive';
import { CdFormValidationDirective } from './ng-bootstrap-form-validation/cd-form-validation.directive';
import { PasswordButtonDirective } from './password-button.directive';
import { StatefulTabDirective } from './stateful-tab.directive';
import { TrimDirective } from './trim.directive';
import { RequiredFieldDirective } from './required-field.directive';
import { ReactiveFormsModule } from '@angular/forms';
import { OptionalFieldDirective } from './optional-field.directive';
import { DimlessBinaryPerMinuteDirective } from './dimless-binary-per-minute.directive';
import { DynamicInputComboboxDirective } from './dynamic-input-combobox.directive';

@NgModule({
  imports: [ReactiveFormsModule],
  declarations: [
    AutofocusDirective,
    DimlessBinaryDirective,
    DimlessBinaryPerSecondDirective,
    PasswordButtonDirective,
    TrimDirective,
    MillisecondsDirective,
    IopsDirective,
    FormLoadingDirective,
    StatefulTabDirective,
    FormInputDisableDirective,
    FormScopeDirective,
    CdFormControlDirective,
    CdFormGroupDirective,
    CdFormValidationDirective,
    AuthStorageDirective,
    RequiredFieldDirective,
    OptionalFieldDirective,
    DimlessBinaryPerMinuteDirective,
    DynamicInputComboboxDirective
  ],
  exports: [
    AutofocusDirective,
    DimlessBinaryDirective,
    DimlessBinaryPerSecondDirective,
    PasswordButtonDirective,
    TrimDirective,
    MillisecondsDirective,
    IopsDirective,
    FormLoadingDirective,
    StatefulTabDirective,
    FormInputDisableDirective,
    FormScopeDirective,
    CdFormControlDirective,
    CdFormGroupDirective,
    CdFormValidationDirective,
    AuthStorageDirective,
    RequiredFieldDirective,
    OptionalFieldDirective,
    DimlessBinaryPerMinuteDirective,
    DynamicInputComboboxDirective
  ]
})
export class DirectivesModule {}
