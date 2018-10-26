import { fakeAsync, tick } from '@angular/core/testing';
import { AbstractControl, FormControl, FormGroup } from '@angular/forms';

import { of as observableOf } from 'rxjs';

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

  describe('ip validator', () => {
    let form: FormGroup;

    beforeEach(() => {
      form = new FormGroup({
        x: new FormControl()
      });
    });

    it('should not error on empty IPv4 addresses', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(4));
      x.setValue('');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should accept valid IPv4 address', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(4));
      x.setValue('19.117.23.141');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should error on IPv4 address containing whitespace', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(4));
      x.setValue('155.144.133.122 ');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('155. 144.133 .122');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue(' 155.144.133.122');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });

    it('should error on IPv4 address containing invalid char', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(4));
      x.setValue('155.144.eee.122 ');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('155.1?.133 .1&2');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });

    it('should error on IPv4 address containing blocks higher than 255', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(4));
      x.setValue('155.270.133.122 ');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('155.144.133.290 ');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });

    it('should not error on empty IPv6 addresses', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(4));
      x.setValue('');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should accept valid IPv6 address', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(6));
      x.setValue('c4dc:1475:cb0b:24ed:3c80:468b:70cd:1a95');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should error on IPv6 address containing too many blocks', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(6));
      x.setValue('c4dc:14753:cb0b:24ed:3c80:468b:70cd:1a95:a3f3');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });

    it('should error on IPv6 address containing more than 4 digits per block', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(6));
      x.setValue('c4dc:14753:cb0b:24ed:3c80:468b:70cd:1a95');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });

    it('should error on IPv6 address containing whitespace', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(6));
      x.setValue('c4dc:14753:cb0b:24ed:3c80:468b:70cd:1a95 ');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('c4dc:14753 :cb0b:24ed:3c80 :468b:70cd :1a95');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue(' c4dc:14753:cb0b:24ed:3c80:468b:70cd:1a95');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });

    it('should error on IPv6 address containing invalid char', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip(6));
      x.setValue('c4dx:14753:cb0b:24ed:3c80:468b:70cd:1a95 ');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('c4dx:14753:cb0b:24ed:3$80:468b:70cd:1a95 ');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });

    it('should accept valid IPv4/6 addresses if not protocol version is given', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip());
      x.setValue('19.117.23.141');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue('c4dc:1475:cb0b:24ed:3c80:468b:70cd:1a95');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });
  });

  describe('uuid validator', () => {
    let form: FormGroup;

    beforeEach(() => {
      form = new FormGroup({
        x: new FormControl(null, CdValidators.uuid())
      });
    });

    it('should accept empty value', () => {
      const x = form.get('x');
      x.setValue('');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should accept valid version 1 uuid', () => {
      const x = form.get('x');
      x.setValue('171af0b2-c305-11e8-a355-529269fb1459');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should accept valid version 4 uuid', () => {
      const x = form.get('x');
      x.setValue('e33bbcb6-fcc3-40b1-ae81-3f81706a35d5');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should error on uuid containing too many blocks', () => {
      const x = form.get('x');
      x.markAsDirty();
      x.setValue('e33bbcb6-fcc3-40b1-ae81-3f81706a35d5-23d3');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('invalidUuid')).toBeTruthy();
    });

    it('should error on uuid containing too many chars in block', () => {
      const x = form.get('x');
      x.markAsDirty();
      x.setValue('aae33bbcb6-fcc3-40b1-ae81-3f81706a35d5');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('invalidUuid')).toBeTruthy();
    });

    it('should error on uuid containing invalid char', () => {
      const x = form.get('x');
      x.markAsDirty();
      x.setValue('x33bbcb6-fcc3-40b1-ae81-3f81706a35d5');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('invalidUuid')).toBeTruthy();

      x.setValue('$33bbcb6-fcc3-40b1-ae81-3f81706a35d5');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('invalidUuid')).toBeTruthy();
    });
  });

  describe('number validator', () => {
    let form: FormGroup;

    beforeEach(() => {
      form = new FormGroup({
        x: new FormControl()
      });
      form.controls['x'].setValidators(CdValidators.number());
    });

    it('should accept empty value', () => {
      const x = form.get('x');
      x.setValue('');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should accept numbers', () => {
      const x = form.get('x');
      x.setValue(42);
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue(-42);
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue('42');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should error on decimal numbers', () => {
      const x = form.get('x');
      x.setValue(42.3);
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue(-42.3);
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('42.3');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });

    it('should error on chars', () => {
      const x = form.get('x');
      x.setValue('char');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('42char');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });

    it('should error on whitespaces', () => {
      const x = form.get('x');
      x.setValue('42 ');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('4 2');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });
  });

  describe('number validator (without negative values)', () => {
    let form: FormGroup;

    beforeEach(() => {
      form = new FormGroup({
        x: new FormControl()
      });
      form.controls['x'].setValidators(CdValidators.number(false));
    });

    it('should accept positive numbers', () => {
      const x = form.get('x');
      x.setValue(42);
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue('42');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should error on negative numbers', () => {
      const x = form.get('x');
      x.setValue(-42);
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('-42');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });
  });

  describe('decimal number validator', () => {
    let form: FormGroup;

    beforeEach(() => {
      form = new FormGroup({
        x: new FormControl()
      });
      form.controls['x'].setValidators(CdValidators.decimalNumber());
    });

    it('should accept empty value', () => {
      const x = form.get('x');
      x.setValue('');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should accept numbers and decimal numbers', () => {
      const x = form.get('x');
      x.setValue(42);
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue(-42);
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue(42.3);
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue(-42.3);
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue('42');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue('42.3');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should error on chars', () => {
      const x = form.get('x');
      x.setValue('42e');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('e42.3');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });

    it('should error on whitespaces', () => {
      const x = form.get('x');
      x.setValue('42.3 ');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('42 .3');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
    });
  });

  describe('decimal number validator (without negative values)', () => {
    let form: FormGroup;

    beforeEach(() => {
      form = new FormGroup({
        x: new FormControl()
      });
      form.controls['x'].setValidators(CdValidators.decimalNumber(false));
    });

    it('should accept positive numbers and decimals', () => {
      const x = form.get('x');
      x.setValue(42);
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue(42.3);
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue('42');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();

      x.setValue('42.3');
      expect(x.valid).toBeTruthy();
      expect(x.hasError('pattern')).toBeFalsy();
    });

    it('should error on negative numbers and decimals', () => {
      const x = form.get('x');
      x.setValue(-42);
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('-42');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue(-42.3);
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();

      x.setValue('-42.3');
      expect(x.valid).toBeFalsy();
      expect(x.hasError('pattern')).toBeTruthy();
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

  describe('match', () => {
    let form: FormGroup;
    let x: FormControl;
    let y: FormControl;

    beforeEach(() => {
      x = new FormControl('aaa');
      y = new FormControl('aaa');
      form = new FormGroup({
        x: x,
        y: y
      });
    });

    it('should error when values are different', () => {
      y.setValue('aab');
      CdValidators.match('x', 'y')(form);
      expect(x.hasError('match')).toBeFalsy();
      expect(y.hasError('match')).toBeTruthy();
    });

    it('should not error when values are equal', () => {
      CdValidators.match('x', 'y')(form);
      expect(x.hasError('match')).toBeFalsy();
      expect(y.hasError('match')).toBeFalsy();
    });

    it('should unset error when values are equal', () => {
      y.setErrors({ match: true });
      CdValidators.match('x', 'y')(form);
      expect(x.hasError('match')).toBeFalsy();
      expect(y.hasError('match')).toBeFalsy();
      expect(y.valid).toBeTruthy();
    });

    it('should keep other existing errors', () => {
      y.setErrors({ match: true, notUnique: true });
      CdValidators.match('x', 'y')(form);
      expect(x.hasError('match')).toBeFalsy();
      expect(y.hasError('match')).toBeFalsy();
      expect(y.hasError('notUnique')).toBeTruthy();
      expect(y.valid).toBeFalsy();
    });
  });

  describe('unique', () => {
    let form: FormGroup;
    let x: AbstractControl;

    beforeEach(() => {
      form = new FormGroup({
        x: new FormControl(
          '',
          null,
          CdValidators.unique((value) => {
            return observableOf('xyz' === value);
          })
        )
      });
      x = form.get('x');
      x.markAsDirty();
    });

    it('should not error because of empty input', () => {
      x.setValue('');
      expect(x.hasError('notUnique')).toBeFalsy();
      expect(x.valid).toBeTruthy();
    });

    it(
      'should not error because of not existing input',
      fakeAsync(() => {
        x.setValue('abc');
        tick(500);
        expect(x.hasError('notUnique')).toBeFalsy();
        expect(x.valid).toBeTruthy();
      })
    );

    it(
      'should error because of already existing input',
      fakeAsync(() => {
        x.setValue('xyz');
        tick(500);
        expect(x.hasError('notUnique')).toBeTruthy();
        expect(x.valid).toBeFalsy();
      })
    );
  });
});
