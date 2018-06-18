import { AbstractControl, ValidationErrors, ValidatorFn, Validators } from '@angular/forms';

import * as _ from 'lodash';

export function isEmptyInputValue(value: any): boolean {
  return value == null || value.length === 0;
}

export class CdValidators {
  /**
   * Validator that performs email validation. In contrast to the Angular
   * email validator an empty email will not be handled as invalid.
   */
  static email(control: AbstractControl): ValidationErrors | null {
    // Exit immediately if value is empty.
    if (isEmptyInputValue(control.value)) {
      return null;
    }
    return Validators.email(control);
  }

  /**
   * Validator that requires controls to fulfill the specified condition if
   * the specified prerequisites matches. If the prerequisites are fulfilled,
   * then the given function is executed and if it succeeds, the 'required'
   * validation error will be returned, otherwise null.
   * @param {Object} prerequisites An object containing the prerequisites.
   *   ### Example
   *   ```typescript
   *   {
   *     'generate_key': true,
   *     'username': 'Max Mustermann'
   *   }
   *   ```
   *   Only if all prerequisites are fulfilled, then the validation of the
   *   control will be triggered.
   * @param {Function | undefined} condition The function to be executed when all
   *   prerequisites are fulfilled. If not set, then the {@link isEmptyInputValue}
   *   function will be used by default. The control's value is used as function
   *   argument. The function must return true to set the validation error.
   * @return {ValidatorFn} Returns the validator function.
   */
  static requiredIf(prerequisites: Object, condition?: Function | undefined): ValidatorFn {
    return (control: AbstractControl): ValidationErrors | null => {
      // Check if all prerequisites matches.
      if (
        !Object.keys(prerequisites).every((key) => {
          return control.parent && control.parent.get(key).value === prerequisites[key];
        })
      ) {
        return null;
      }
      const success = _.isFunction(condition)
        ? condition.call(condition, control.value)
        : isEmptyInputValue(control.value);
      return success ? { required: true } : null;
    };
  }

  /**
   * Custom validation by passing a name for the error and a function as error condition.
   *
   * @param {string} error
   * @param {Function} condition - a truthy return value will trigger the error
   * @returns {ValidatorFn}
   */
  static custom(error: string, condition: Function): ValidatorFn {
    return (control: AbstractControl): { [key: string]: any } => {
      const value = condition.call(this, control.value);
      if (value) {
        return { [error]: value };
      }
      return null;
    };
  }

  /**
   * Validate form control if condition is true with validators.
   *
   * @param {AbstractControl} formControl
   * @param {Function} condition
   * @param {ValidatorFn[]} validators
   */
  static validateIf(
    formControl: AbstractControl,
    condition: Function,
    validators: ValidatorFn[]
  ) {
    formControl.setValidators((control: AbstractControl): {
      [key: string]: any;
    } => {
      const value = condition.call(this);
      if (value) {
        return Validators.compose(validators)(control);
      }
      return null;
    });
  }

  /**
   * Validate if control1 and control2 have the same value.
   * Error will be added to control2.
   *
   * @param {string} control1
   * @param {string} control2
   */
  static match(control1: string, control2: string): ValidatorFn {
    return (control: AbstractControl): { [key: string]: any } => {
      if (control.get(control1).value !== control.get(control2).value) {
        control.get(control2).setErrors({ ['match']: true });
      }
      return null;
    };
  }
}
