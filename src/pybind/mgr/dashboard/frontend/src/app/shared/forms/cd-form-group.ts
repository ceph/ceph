import {
  AbstractControl,
  AbstractControlOptions,
  AsyncValidatorFn,
  UntypedFormGroup,
  NgForm,
  ValidatorFn
} from '@angular/forms';

/**
 * CdFormGroup extends FormGroup with a few new methods that will help form development.
 */
export class CdFormGroup extends UntypedFormGroup {
  constructor(
    public controls: { [key: string]: AbstractControl },
    validatorOrOpts?: ValidatorFn | ValidatorFn[] | AbstractControlOptions | null,
    asyncValidator?: AsyncValidatorFn | AsyncValidatorFn[] | null
  ) {
    super(controls, validatorOrOpts, asyncValidator);
  }

  /**
   * Get a control out of any control even if its nested in other CdFormGroups or a FormGroup
   */
  get(controlName: string): AbstractControl {
    const control = this._get(controlName);
    if (!control) {
      throw new Error(`Control '${controlName}' could not be found!`);
    }
    return control;
  }

  _get(controlName: string): AbstractControl {
    return (
      super.get(controlName) ||
      Object.values(this.controls)
        .filter((c) => c.get)
        .map((form) => {
          if (form instanceof CdFormGroup) {
            return form._get(controlName);
          }
          return form.get(controlName);
        })
        .find((c) => Boolean(c))
    );
  }

  /**
   * Get the value of a control
   */
  getValue(controlName: string): any {
    return this.get(controlName).value;
  }

  /**
   * Sets a control without triggering a value changes event
   *
   * Very useful if a function is called through a value changes event but the value
   * should be changed within the call.
   */
  silentSet(controlName: string, value: any) {
    this.get(controlName).setValue(value, { emitEvent: false });
  }

  /**
   * Indicates errors of the control in templates
   */
  showError(controlName: string, form: NgForm, errorName?: string): boolean {
    const control = this.get(controlName);
    return (
      (form?.submitted || control.dirty) &&
      (errorName ? control.hasError(errorName) : control.invalid)
    );
  }
}
