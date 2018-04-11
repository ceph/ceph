import {
  AbstractControl,
  ValidationErrors,
  ValidatorFn,
  Validators
} from '@angular/forms';

import * as _ from 'lodash';

type Prerequisites = { // tslint:disable-line
  [key: string]: any
};

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
   * @param {Prerequisites} prerequisites An object containing the prerequisites.
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
  static requiredIf(prerequisites: Prerequisites, condition?: Function | undefined): ValidatorFn {
    return (control: AbstractControl): ValidationErrors | null => {
      // Check if all prerequisites matches.
      if (!Object.keys(prerequisites).every((key) => {
        return (control.parent && control.parent.get(key).value === prerequisites[key]);
      })) {
        return null;
      }
      const success = _.isFunction(condition) ? condition.call(condition, control.value) :
        isEmptyInputValue(control.value);
      return success ? {'required': true} : null;
    };
  }
}
