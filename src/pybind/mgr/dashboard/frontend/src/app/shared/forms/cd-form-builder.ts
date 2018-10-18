import { Injectable } from '@angular/core';
import { FormBuilder } from '@angular/forms';

import { ServicesModule } from '../services/services.module';
import { CdFormGroup } from './cd-form-group';

/**
 * CdFormBuilder extends FormBuilder to create an CdFormGroup based form.
 */
@Injectable({
  providedIn: ServicesModule
})
export class CdFormBuilder extends FormBuilder {
  group(
    controlsConfig: { [key: string]: any },
    extra: { [key: string]: any } | null = null
  ): CdFormGroup {
    const form = super.group(controlsConfig, extra);
    return new CdFormGroup(form.controls, form.validator, form.asyncValidator);
  }
}
