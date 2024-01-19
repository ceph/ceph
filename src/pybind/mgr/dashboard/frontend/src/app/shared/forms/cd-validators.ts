import {
  AbstractControl,
  AsyncValidatorFn,
  ValidationErrors,
  ValidatorFn,
  Validators
} from '@angular/forms';

import _ from 'lodash';
import { Observable, of as observableOf, timer as observableTimer } from 'rxjs';
import { map, switchMapTo, take } from 'rxjs/operators';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { DimlessBinaryPipe } from '~/app/shared/pipes/dimless-binary.pipe';
import { FormatterService } from '~/app/shared/services/formatter.service';

export function isEmptyInputValue(value: any): boolean {
  return value == null || value.length === 0;
}

export type existsServiceFn = (value: any, ...args: any[]) => Observable<boolean>;

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
   * Validator function in order to validate IP addresses.
   * @param {number} version determines the protocol version. It needs to be set to 4 for IPv4 and
   *   to 6 for IPv6 validation. For any other number (it's also the default case) it will return a
   *   function to validate the input string against IPv4 OR IPv6.
   * @returns {ValidatorFn} A validator function that returns an error map containing `pattern`
   *   if the validation check fails, otherwise `null`.
   */
  static ip(version: number = 0): ValidatorFn {
    // prettier-ignore
    const ipv4Rgx =
      /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
    const ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;

    if (version === 4) {
      return Validators.pattern(ipv4Rgx);
    } else if (version === 6) {
      return Validators.pattern(ipv6Rgx);
    } else {
      return Validators.pattern(new RegExp(ipv4Rgx.source + '|' + ipv6Rgx.source));
    }
  }

  /**
   * Validator function in order to validate numbers.
   * @returns {ValidatorFn} A validator function that returns an error map containing `pattern`
   *   if the validation check fails, otherwise `null`.
   */
  static number(allowsNegative: boolean = true): ValidatorFn {
    if (allowsNegative) {
      return Validators.pattern(/^-?[0-9]+$/i);
    } else {
      return Validators.pattern(/^[0-9]+$/i);
    }
  }

  /**
   * Validator function in order to validate decimal numbers.
   * @returns {ValidatorFn} A validator function that returns an error map containing `pattern`
   *   if the validation check fails, otherwise `null`.
   */
  static decimalNumber(allowsNegative: boolean = true): ValidatorFn {
    if (allowsNegative) {
      return Validators.pattern(/^-?[0-9]+(.[0-9]+)?$/i);
    } else {
      return Validators.pattern(/^[0-9]+(.[0-9]+)?$/i);
    }
  }

  /**
   * Validator that performs SSL certificate validation.
   * @returns {ValidatorFn} A validator function that returns an error map containing `pattern`
   *   if the validation check fails, otherwise `null`.
   */
  static sslCert(): ValidatorFn {
    return Validators.pattern(
      /^-----BEGIN CERTIFICATE-----(\n|\r|\f)((.+)?((\n|\r|\f).+)*)(\n|\r|\f)-----END CERTIFICATE-----[\n\r\f]*$/
    );
  }

  /**
   * Validator that performs SSL private key validation.
   * @returns {ValidatorFn} A validator function that returns an error map containing `pattern`
   *   if the validation check fails, otherwise `null`.
   */
  static sslPrivKey(): ValidatorFn {
    return Validators.pattern(
      /^-----BEGIN RSA PRIVATE KEY-----(\n|\r|\f)((.+)?((\n|\r|\f).+)*)(\n|\r|\f)-----END RSA PRIVATE KEY-----[\n\r\f]*$/
    );
  }

  /**
   * Validator that performs SSL certificate validation of pem format.
   * @returns {ValidatorFn} A validator function that returns an error map containing `pattern`
   *   if the validation check fails, otherwise `null`.
   */
  static pemCert(): ValidatorFn {
    return Validators.pattern(/^-----BEGIN .+-----$.+^-----END .+-----$/ms);
  }

  /**
   * Validator that requires controls to fulfill the specified condition if
   * the specified prerequisites matches. If the prerequisites are fulfilled,
   * then the given function is executed and if it succeeds, the 'required'
   * validation error will be returned, otherwise null.
   * @param {Object} prerequisites An object containing the prerequisites.
   *   To do additional checks rather than checking for equality you can
   *   use the extended prerequisite syntax:
   *     'field_name': { 'op': '<OPERATOR>', arg1: '<OPERATOR_ARGUMENT>' }
   *   The following operators are supported:
   *   * empty
   *   * !empty
   *   * equal
   *   * !equal
   *   * minLength
   *   ### Example
   *   ```typescript
   *   {
   *     'generate_key': true,
   *     'username': 'Max Mustermann'
   *   }
   *   ```
   *   ### Example - Extended prerequisites
   *   ```typescript
   *   {
   *     'generate_key': { 'op': 'equal', 'arg1': true },
   *     'username': { 'op': 'minLength', 'arg1': 5 }
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
  static requiredIf(prerequisites: object, condition?: Function | undefined): ValidatorFn {
    let isWatched = false;

    return (control: AbstractControl): ValidationErrors | null => {
      if (!isWatched && control.parent) {
        Object.keys(prerequisites).forEach((key) => {
          control.parent.get(key).valueChanges.subscribe(() => {
            control.updateValueAndValidity({ emitEvent: false });
          });
        });

        isWatched = true;
      }

      // Check if all prerequisites met.
      if (
        !Object.keys(prerequisites).every((key) => {
          if (!control.parent) {
            return false;
          }
          const value = control.parent.get(key).value;
          const prerequisite = prerequisites[key];
          if (_.isObjectLike(prerequisite)) {
            let result = false;
            switch (prerequisite['op']) {
              case 'empty':
                result = _.isEmpty(value);
                break;
              case '!empty':
                result = !_.isEmpty(value);
                break;
              case 'equal':
                result = value === prerequisite['arg1'];
                break;
              case '!equal':
                result = value !== prerequisite['arg1'];
                break;
              case 'minLength':
                if (_.isString(value)) {
                  result = value.length >= prerequisite['arg1'];
                }
                break;
            }
            return result;
          }
          return value === prerequisite;
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
   * Compose multiple validators into a single function that returns the union of
   * the individual error maps for the provided control when the given prerequisites
   * are fulfilled.
   *
   * @param {Object} prerequisites An object containing the prerequisites as
   *   key/value pairs.
   *   ### Example
   *   ```typescript
   *   {
   *     'generate_key': true,
   *     'username': 'Max Mustermann'
   *   }
   *   ```
   * @param {ValidatorFn[]} validators List of validators that should be taken
   *   into action when the prerequisites are met.
   * @return {ValidatorFn} Returns the validator function.
   */
  static composeIf(prerequisites: object, validators: ValidatorFn[]): ValidatorFn {
    let isWatched = false;
    return (control: AbstractControl): ValidationErrors | null => {
      if (!isWatched && control.parent) {
        Object.keys(prerequisites).forEach((key) => {
          control.parent.get(key).valueChanges.subscribe(() => {
            control.updateValueAndValidity({ emitEvent: false });
          });
        });
        isWatched = true;
      }
      // Check if all prerequisites are met.
      if (
        !Object.keys(prerequisites).every((key) => {
          return control.parent && control.parent.get(key).value === prerequisites[key];
        })
      ) {
        return null;
      }
      return Validators.compose(validators)(control);
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
   * @param {ValidatorFn[]} conditionalValidators List of validators that should only be tested
   * when the condition is met
   * @param {ValidatorFn[]} permanentValidators List of validators that should always be tested
   * @param {AbstractControl[]} watchControls List of controls that the condition depend on.
   * Every time one of this controls value is updated, the validation will be triggered
   */
  static validateIf(
    formControl: AbstractControl,
    condition: Function,
    conditionalValidators: ValidatorFn[],
    permanentValidators: ValidatorFn[] = [],
    watchControls: AbstractControl[] = []
  ) {
    conditionalValidators = conditionalValidators.concat(permanentValidators);

    formControl.setValidators((control: AbstractControl): {
      [key: string]: any;
    } => {
      const value = condition.call(this);
      if (value) {
        return Validators.compose(conditionalValidators)(control);
      }
      if (permanentValidators.length > 0) {
        return Validators.compose(permanentValidators)(control);
      }
      return null;
    });

    watchControls.forEach((control: AbstractControl) => {
      control.valueChanges.subscribe(() => {
        formControl.updateValueAndValidity({ emitEvent: false });
      });
    });
  }

  /**
   * Validator that requires that both specified controls have the same value.
   * Error will be added to the `path2` control.
   * @param {string} path1 A dot-delimited string that define the path to the control.
   * @param {string} path2 A dot-delimited string that define the path to the control.
   * @return {ValidatorFn} Returns a validator function that always returns `null`.
   *   If the validation fails an error map with the `match` property will be set
   *   on the `path2` control.
   */
  static match(path1: string, path2: string): ValidatorFn {
    return (control: AbstractControl): { [key: string]: any } => {
      const ctrl1 = control.get(path1);
      const ctrl2 = control.get(path2);
      if (!ctrl1 || !ctrl2) {
        return null;
      }
      if (ctrl1.value !== ctrl2.value) {
        ctrl2.setErrors({ match: true });
      } else {
        const hasError = ctrl2.hasError('match');
        if (hasError) {
          // Remove the 'match' error. If no more errors exists, then set
          // the error value to 'null', otherwise the field is still marked
          // as invalid.
          const errors = ctrl2.errors;
          _.unset(errors, 'match');
          ctrl2.setErrors(_.isEmpty(_.keys(errors)) ? null : errors);
        }
      }
      return null;
    };
  }

  /**
   * Asynchronous validator that requires the control's value to be unique.
   * The validation is only executed after the specified delay. Every
   * keystroke during this delay will restart the timer.
   * @param serviceFn {existsServiceFn} The service function that is
   *   called to check whether the given value exists. It must return
   *   boolean 'true' if the given value exists, otherwise 'false'.
   * @param serviceFnThis {any} The object to be used as the 'this' object
   *   when calling the serviceFn function. Defaults to null.
   * @param {number|Date} dueTime The delay time to wait before the
   *   serviceFn call is executed. This is useful to prevent calls on
   *   every keystroke. Defaults to 500.
   * @return {AsyncValidatorFn} Returns an asynchronous validator function
   *   that returns an error map with the `notUnique` property if the
   *   validation check succeeds, otherwise `null`.
   */
  static unique(
    serviceFn: existsServiceFn,
    serviceFnThis: any = null,
    usernameFn?: Function,
    uidField = false,
    ...extraArgs: any[]
  ): AsyncValidatorFn {
    let uName: string;
    return (control: AbstractControl): Observable<ValidationErrors | null> => {
      // Exit immediately if user has not interacted with the control yet
      // or the control value is empty.
      if (control.pristine || isEmptyInputValue(control.value)) {
        return observableOf(null);
      }
      uName = control.value;
      if (_.isFunction(usernameFn) && usernameFn() !== null && usernameFn() !== '') {
        if (uidField) {
          uName = `${control.value}$${usernameFn()}`;
        } else {
          uName = `${usernameFn()}$${control.value}`;
        }
      }

      return observableTimer().pipe(
        switchMapTo(serviceFn.call(serviceFnThis, uName, ...extraArgs)),
        map((resp: boolean) => {
          if (!resp) {
            return null;
          } else {
            return { notUnique: true };
          }
        }),
        take(1)
      );
    };
  }

  /**
   * Validator function for UUIDs.
   * @param required - Defines if it is mandatory to fill in the UUID
   * @return Validator function that returns an error object containing `invalidUuid` if the
   * validation failed, `null` otherwise.
   */
  static uuid(required = false): ValidatorFn {
    const uuidRe = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    return (control: AbstractControl): { [key: string]: any } | null => {
      if (control.pristine && control.untouched) {
        return null;
      } else if (!required && !control.value) {
        return null;
      } else if (uuidRe.test(control.value)) {
        return null;
      }
      return { invalidUuid: 'This is not a valid UUID' };
    };
  }

  /**
   * A simple minimum validator vor cd-binary inputs.
   *
   * To use the validation message pass I18n into the function as it cannot
   * be called in a static one.
   */
  static binaryMin(bytes: number): ValidatorFn {
    return (control: AbstractControl): { [key: string]: () => string } | null => {
      const formatterService = new FormatterService();
      const currentBytes = new FormatterService().toBytes(control.value);
      if (bytes <= currentBytes) {
        return null;
      }
      const value = new DimlessBinaryPipe(formatterService).transform(bytes);
      return {
        binaryMin: () => $localize`Size has to be at least ${value} or more`
      };
    };
  }

  /**
   * A simple maximum validator vor cd-binary inputs.
   *
   * To use the validation message pass I18n into the function as it cannot
   * be called in a static one.
   */
  static binaryMax(bytes: number): ValidatorFn {
    return (control: AbstractControl): { [key: string]: () => string } | null => {
      const formatterService = new FormatterService();
      const currentBytes = formatterService.toBytes(control.value);
      if (bytes >= currentBytes) {
        return null;
      }
      const value = new DimlessBinaryPipe(formatterService).transform(bytes);
      return {
        binaryMax: () => $localize`Size has to be at most ${value} or less`
      };
    };
  }

  /**
   * Asynchronous validator that checks if the password meets the password
   * policy.
   * @param userServiceThis The object to be used as the 'this' object
   *   when calling the 'validatePassword' method of the 'UserService'.
   * @param usernameFn Function to get the username that should be
   *   taken into account.
   * @param callback Callback function that is called after the validation
   *   has been done.
   * @return {AsyncValidatorFn} Returns an asynchronous validator function
   *   that returns an error map with the `passwordPolicy` property if the
   *   validation check fails, otherwise `null`.
   */
  static passwordPolicy(
    userServiceThis: any,
    usernameFn?: Function,
    callback?: (valid: boolean, credits?: number, valuation?: string) => void
  ): AsyncValidatorFn {
    return (control: AbstractControl): Observable<ValidationErrors | null> => {
      if (control.pristine || control.value === '') {
        if (_.isFunction(callback)) {
          callback(true, 0);
        }
        return observableOf(null);
      }
      let username;
      if (_.isFunction(usernameFn)) {
        username = usernameFn();
      }
      return observableTimer(500).pipe(
        switchMapTo(_.invoke(userServiceThis, 'validatePassword', control.value, username)),
        map((resp: { valid: boolean; credits: number; valuation: string }) => {
          if (_.isFunction(callback)) {
            callback(resp.valid, resp.credits, resp.valuation);
          }
          if (resp.valid) {
            return null;
          } else {
            return { passwordPolicy: true };
          }
        }),
        take(1)
      );
    };
  }

  /**
   * Validate the bucket name. In general, bucket names should follow domain
   * name constraints:
   * - Bucket names must be unique.
   * - Bucket names cannot be formatted as IP address.
   * - Bucket names can be between 3 and 63 characters long.
   * - Bucket names must not contain uppercase characters or underscores.
   * - Bucket names must start with a lowercase letter or number.
   * - Bucket names must be a series of one or more labels. Adjacent
   *   labels are separated by a single period (.). Bucket names can
   *   contain lowercase letters, numbers, and hyphens. Each label must
   *   start and end with a lowercase letter or a number.
   */
  static bucketName(): AsyncValidatorFn {
    return (control: AbstractControl): Observable<ValidationErrors | null> => {
      if (control.pristine || !control.value) {
        return observableOf({ required: true });
      }
      const constraints = [];
      let errorName: string;
      // - Bucket names cannot be formatted as IP address.
      constraints.push(() => {
        const ipv4Rgx = /^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/i;
        const ipv6Rgx = /^(?:[a-f0-9]{1,4}:){7}[a-f0-9]{1,4}$/i;
        const name = control.value;
        let notIP = true;
        if (ipv4Rgx.test(name) || ipv6Rgx.test(name)) {
          errorName = 'ipAddress';
          notIP = false;
        }
        return notIP;
      });
      // - Bucket names can be between 3 and 63 characters long.
      constraints.push((name: string) => {
        if (!_.inRange(name.length, 3, 64)) {
          errorName = 'shouldBeInRange';
          return false;
        }
        // Bucket names can only contain lowercase letters, numbers, periods and hyphens.
        if (!/^[0-9a-z.-]+$/.test(control.value)) {
          errorName = 'bucketNameInvalid';
          return false;
        }
        return true;
      });
      // - Bucket names must not contain uppercase characters or underscores.
      // - Bucket names must start with a lowercase letter or number.
      // - Bucket names must be a series of one or more labels. Adjacent
      //   labels are separated by a single period (.). Bucket names can
      //   contain lowercase letters, numbers, and hyphens. Each label must
      //   start and end with a lowercase letter or a number.
      constraints.push((name: string) => {
        const labels = _.split(name, '.');
        return _.every(labels, (label) => {
          // Bucket names must not contain uppercase characters or underscores.
          if (label !== _.toLower(label) || label.includes('_')) {
            errorName = 'containsUpperCase';
            return false;
          }
          // Bucket labels can contain lowercase letters, numbers, and hyphens.
          if (!/^[0-9a-z-]+$/.test(label)) {
            errorName = 'onlyLowerCaseAndNumbers';
            return false;
          }
          // Each label must start and end with a lowercase letter or a number.
          return _.every([0, label.length - 1], (index) => {
            errorName = 'lowerCaseOrNumber';
            return /[a-z]/.test(label[index]) || _.isInteger(_.parseInt(label[index]));
          });
        });
      });
      if (!_.every(constraints, (func: Function) => func(control.value))) {
        return observableOf(
          (() => {
            switch (errorName) {
              case 'onlyLowerCaseAndNumbers':
                return { onlyLowerCaseAndNumbers: true };
              case 'shouldBeInRange':
                return { shouldBeInRange: true };
              case 'ipAddress':
                return { ipAddress: true };
              case 'containsUpperCase':
                return { containsUpperCase: true };
              case 'lowerCaseOrNumber':
                return { lowerCaseOrNumber: true };
              default:
                return { bucketNameInvalid: true };
            }
          })()
        );
      }

      return observableOf(null);
    };
  }

  static bucketExistence(
    requiredExistenceResult: boolean,
    rgwBucketService: RgwBucketService
  ): AsyncValidatorFn {
    return (control: AbstractControl): Observable<ValidationErrors | null> => {
      if (control.pristine || !control.value) {
        return observableOf({ required: true });
      }
      return rgwBucketService
        .exists(control.value)
        .pipe(
          map((existenceResult: boolean) =>
            existenceResult === requiredExistenceResult ? null : { bucketNameNotAllowed: true }
          )
        );
    };
  }
}
