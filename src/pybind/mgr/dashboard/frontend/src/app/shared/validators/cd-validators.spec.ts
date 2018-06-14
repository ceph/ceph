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
      expect(CdValidators.email(control)).toEqual({ email: true });
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
        x: true,
        y: 'abc'
      });
      expect(validatorFn(form.controls['z'])).toBeNull();
    });

    it('should not error because of unmet prerequisites', () => {
      // Define prereqs that do not match the current values of the form fields.
      const validatorFn = CdValidators.requiredIf({
        x: false,
        y: 'xyz'
      });
      // The validator must succeed because the prereqs do not match, so the
      // validation of the 'z' control will be skipped.
      expect(validatorFn(form.controls['z'])).toBeNull();
    });

    it('should error because of an empty value', () => {
      // Define prereqs that force the validator to validate the value of
      // the 'z' control.
      const validatorFn = CdValidators.requiredIf({
        x: true,
        y: 'abc'
      });
      // The validator must fail because the value of control 'z' is empty.
      expect(validatorFn(form.controls['z'])).toEqual({ required: true });
    });

    it('should not error because of unsuccessful condition', () => {
      form.get('z').setValue('zyx');
      // Define prereqs that force the validator to validate the value of
      // the 'z' control.
      const validatorFn = CdValidators.requiredIf(
        {
          x: true,
          z: 'zyx'
        },
        () => false
      );
      expect(validatorFn(form.controls['z'])).toBeNull();
    });

    it('should error because of successful condition', () => {
      const conditionFn = (value) => {
        return value === 'abc';
      };
      // Define prereqs that force the validator to validate the value of
      // the 'y' control.
      const validatorFn = CdValidators.requiredIf(
        {
          x: true,
          z: ''
        },
        conditionFn
      );
      expect(validatorFn(form.controls['y'])).toEqual({ required: true });
    });
  });

  describe('custom validation', () => {
    let form: FormGroup;

    beforeEach(() => {
      form = new FormGroup({
        x: new FormControl(3, CdValidators.custom('odd', (x) => x % 2 === 1)),
        y: new FormControl(
          5,
          CdValidators.custom('not-dividable-by-x', (y) => {
            const x = (form && form.get('x').value) || 1;
            return y % x !== 0;
          })
        )
      });
    });

    it('should test error and valid condition for odd x', () => {
      const x = form.get('x');
      x.updateValueAndValidity();
      expect(x.hasError('odd')).toBeTruthy();
      x.setValue(4);
      expect(x.valid).toBeTruthy();
    });

    it('should test error and valid condition for y if its dividable by x', () => {
      const y = form.get('y');
      y.updateValueAndValidity();
      expect(y.hasError('not-dividable-by-x')).toBeTruthy();
      y.setValue(6);
      y.updateValueAndValidity();
      expect(y.valid).toBeTruthy();
    });
  });

  describe('validate if condition', () => {
    let form: FormGroup;

    beforeEach(() => {
      form = new FormGroup({
        x: new FormControl(3),
        y: new FormControl(5)
      });
      CdValidators.validateIf(form.get('x'), () => ((form && form.get('y').value) || 0) > 10, [
        CdValidators.custom('min', (x) => x < 7),
        CdValidators.custom('max', (x) => x > 12)
      ]);
    });

    it('should test min error', () => {
      const x = form.get('x');
      const y = form.get('y');
      expect(x.valid).toBeTruthy();
      y.setValue(11);
      x.updateValueAndValidity();
      expect(x.hasError('min')).toBeTruthy();
    });

    it('should test max error', () => {
      const x = form.get('x');
      const y = form.get('y');
      expect(x.valid).toBeTruthy();
      y.setValue(11);
      x.setValue(13);
      expect(x.hasError('max')).toBeTruthy();
    });

    it('should test valid number with validation', () => {
      const x = form.get('x');
      const y = form.get('y');
      expect(x.valid).toBeTruthy();
      y.setValue(11);
      x.setValue(12);
      expect(x.valid).toBeTruthy();
    });
  });
});
