import { FormControl, FormGroup } from '@angular/forms';

import { CdValidators } from './cd-validators';

describe('CdValidators', () => {
  describe('email', () => {
    it('should not error on an empty email address', () => {
      const control = new FormControl('');
      expect(CdValidators.email(control)).toBeNull();
    });

    it('should not error on valid email address', () => {
      const control = new FormControl('dashboard@ceph.com');
      expect(CdValidators.email(control)).toBeNull();
    });

    it('should error on invalid email address', () => {
      const control = new FormControl('xyz');
      expect(CdValidators.email(control)).toEqual({'email': true});
    });
  });

  describe('requiredIf', () => {
    let form: FormGroup;

    beforeEach(() => {
      form = new FormGroup({
        x: new FormControl(true),
        y: new FormControl('abc'),
        z: new FormControl('')
      });
    });

    it('should not error because all conditions are fulfilled', () => {
      form.get('z').setValue('zyx');
      const validatorFn = CdValidators.requiredIf({
        'x': true,
        'y': 'abc'
      });
      expect(validatorFn(form.controls['z'])).toBeNull();
    });

    it('should not error because of unmet prerequisites', () => {
      // Define prereqs that do not match the current values of the form fields.
      const validatorFn = CdValidators.requiredIf({
        'x': false,
        'y': 'xyz'
      });
      // The validator must succeed because the prereqs do not match, so the
      // validation of the 'z' control will be skipped.
      expect(validatorFn(form.controls['z'])).toBeNull();
    });

    it('should error because of an empty value', () => {
      // Define prereqs that force the validator to validate the value of
      // the 'z' control.
      const validatorFn = CdValidators.requiredIf({
        'x': true,
        'y': 'abc'
      });
      // The validator must fail because the value of control 'z' is empty.
      expect(validatorFn(form.controls['z'])).toEqual({'required': true});
    });

    it('should not error because of unsuccessful condition', () => {
      form.get('z').setValue('zyx');
      // Define prereqs that force the validator to validate the value of
      // the 'z' control.
      const validatorFn = CdValidators.requiredIf({
        'x': true,
        'z': 'zyx'
      }, () => false);
      expect(validatorFn(form.controls['z'])).toBeNull();
    });

    it('should error because of successful condition', () => {
      const conditionFn = (value) => {
        return value === 'abc';
      };
      // Define prereqs that force the validator to validate the value of
      // the 'y' control.
      const validatorFn = CdValidators.requiredIf({
        'x': true,
        'z': ''
      }, conditionFn);
      expect(validatorFn(form.controls['y'])).toEqual({'required': true});
    });
  });
});
