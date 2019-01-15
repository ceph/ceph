import { fakeAsync, tick } from '@angular/core/testing';
import { FormControl, Validators } from '@angular/forms';

import { of as observableOf } from 'rxjs';

import { FormHelper } from '../../../testing/unit-test-helper';
import { CdFormGroup } from './cd-form-group';
import { CdValidators } from './cd-validators';

describe('CdValidators', () => {
  let formHelper: FormHelper;
  let form: CdFormGroup;

  const expectValid = (value) => formHelper.expectValidChange('x', value);
  const expectPatternError = (value) => formHelper.expectErrorChange('x', value, 'pattern');
  const updateValidity = (controlName) => form.get(controlName).updateValueAndValidity();

  beforeEach(() => {
    form = new CdFormGroup({
      x: new FormControl()
    });
    formHelper = new FormHelper(form);
  });

  describe('email', () => {
    beforeEach(() => {
      form.get('x').setValidators(CdValidators.email);
    });

    it('should not error on an empty email address', () => {
      expectValid('');
    });

    it('should not error on valid email address', () => {
      expectValid('dashboard@ceph.com');
    });

    it('should error on invalid email address', () => {
      formHelper.expectErrorChange('x', 'xyz', 'email');
    });
  });

  describe('ip validator', () => {
    describe('IPv4', () => {
      beforeEach(() => {
        form.get('x').setValidators(CdValidators.ip(4));
      });

      it('should not error on empty addresses', () => {
        expectValid('');
      });

      it('should accept valid address', () => {
        expectValid('19.117.23.141');
      });

      it('should error containing whitespace', () => {
        expectPatternError('155.144.133.122 ');
        expectPatternError('155. 144.133 .122');
        expectPatternError(' 155.144.133.122');
      });

      it('should error containing invalid char', () => {
        expectPatternError('155.144.eee.122 ');
        expectPatternError('155.1?.133 .1&2');
      });

      it('should error containing blocks higher than 255', () => {
        expectPatternError('155.270.133.122');
        expectPatternError('155.144.133.290');
      });
    });

    describe('IPv4', () => {
      beforeEach(() => {
        form.get('x').setValidators(CdValidators.ip(6));
      });

      it('should not error on empty IPv6 addresses', () => {
        expectValid('');
      });

      it('should accept valid IPv6 address', () => {
        expectValid('c4dc:1475:cb0b:24ed:3c80:468b:70cd:1a95');
      });

      it('should error on IPv6 address containing too many blocks', () => {
        formHelper.expectErrorChange(
          'x',
          'c4dc:14753:cb0b:24ed:3c80:468b:70cd:1a95:a3f3',
          'pattern'
        );
      });

      it('should error on IPv6 address containing more than 4 digits per block', () => {
        expectPatternError('c4dc:14753:cb0b:24ed:3c80:468b:70cd:1a95');
      });

      it('should error on IPv6 address containing whitespace', () => {
        expectPatternError('c4dc:14753:cb0b:24ed:3c80:468b:70cd:1a95 ');
        expectPatternError('c4dc:14753 :cb0b:24ed:3c80 :468b:70cd :1a95');
        expectPatternError(' c4dc:14753:cb0b:24ed:3c80:468b:70cd:1a95');
      });

      it('should error on IPv6 address containing invalid char', () => {
        expectPatternError('c4dx:14753:cb0b:24ed:3c80:468b:70cd:1a95');
        expectPatternError('c4da:14753:cb0b:24ed:3$80:468b:70cd:1a95');
      });
    });

    it('should accept valid IPv4/6 addresses if not protocol version is given', () => {
      const x = form.get('x');
      x.setValidators(CdValidators.ip());
      expectValid('19.117.23.141');
      expectValid('c4dc:1475:cb0b:24ed:3c80:468b:70cd:1a95');
    });
  });

  describe('uuid validator', () => {
    const expectUuidError = (value) =>
      formHelper.expectErrorChange('x', value, 'invalidUuid', true);
    beforeEach(() => {
      form.get('x').setValidators(CdValidators.uuid());
    });

    it('should accept empty value', () => {
      expectValid('');
    });

    it('should accept valid version 1 uuid', () => {
      expectValid('171af0b2-c305-11e8-a355-529269fb1459');
    });

    it('should accept valid version 4 uuid', () => {
      expectValid('e33bbcb6-fcc3-40b1-ae81-3f81706a35d5');
    });

    it('should error on uuid containing too many blocks', () => {
      expectUuidError('e33bbcb6-fcc3-40b1-ae81-3f81706a35d5-23d3');
    });

    it('should error on uuid containing too many chars in block', () => {
      expectUuidError('aae33bbcb6-fcc3-40b1-ae81-3f81706a35d5');
    });

    it('should error on uuid containing invalid char', () => {
      expectUuidError('x33bbcb6-fcc3-40b1-ae81-3f81706a35d5');
      expectUuidError('$33bbcb6-fcc3-40b1-ae81-3f81706a35d5');
    });
  });

  describe('number validator', () => {
    beforeEach(() => {
      form.get('x').setValidators(CdValidators.number());
    });

    it('should accept empty value', () => {
      expectValid('');
    });

    it('should accept numbers', () => {
      expectValid(42);
      expectValid(-42);
      expectValid('42');
    });

    it('should error on decimal numbers', () => {
      expectPatternError(42.3);
      expectPatternError(-42.3);
      expectPatternError('42.3');
    });

    it('should error on chars', () => {
      expectPatternError('char');
      expectPatternError('42char');
    });

    it('should error on whitespaces', () => {
      expectPatternError('42 ');
      expectPatternError('4 2');
    });
  });

  describe('number validator (without negative values)', () => {
    beforeEach(() => {
      form.get('x').setValidators(CdValidators.number(false));
    });

    it('should accept positive numbers', () => {
      expectValid(42);
      expectValid('42');
    });

    it('should error on negative numbers', () => {
      expectPatternError(-42);
      expectPatternError('-42');
    });
  });

  describe('decimal number validator', () => {
    beforeEach(() => {
      form.get('x').setValidators(CdValidators.decimalNumber());
    });

    it('should accept empty value', () => {
      expectValid('');
    });

    it('should accept numbers and decimal numbers', () => {
      expectValid(42);
      expectValid(-42);
      expectValid(42.3);
      expectValid(-42.3);
      expectValid('42');
      expectValid('42.3');
    });

    it('should error on chars', () => {
      expectPatternError('42e');
      expectPatternError('e42.3');
    });

    it('should error on whitespaces', () => {
      expectPatternError('42.3 ');
      expectPatternError('42 .3');
    });
  });

  describe('decimal number validator (without negative values)', () => {
    beforeEach(() => {
      form.get('x').setValidators(CdValidators.decimalNumber(false));
    });

    it('should accept positive numbers and decimals', () => {
      expectValid(42);
      expectValid(42.3);
      expectValid('42');
      expectValid('42.3');
    });

    it('should error on negative numbers and decimals', () => {
      expectPatternError(-42);
      expectPatternError('-42');
      expectPatternError(-42.3);
      expectPatternError('-42.3');
    });
  });

  describe('requiredIf', () => {
    beforeEach(() => {
      form = new CdFormGroup({
        x: new FormControl(true),
        y: new FormControl('abc'),
        z: new FormControl('')
      });
      formHelper = new FormHelper(form);
    });

    it('should not error because all conditions are fulfilled', () => {
      formHelper.setValue('z', 'zyx');
      const validatorFn = CdValidators.requiredIf({
        x: true,
        y: 'abc'
      });
      expect(validatorFn(form.get('z'))).toBeNull();
    });

    it('should not error because of unmet prerequisites', () => {
      // Define prereqs that do not match the current values of the form fields.
      const validatorFn = CdValidators.requiredIf({
        x: false,
        y: 'xyz'
      });
      // The validator must succeed because the prereqs do not match, so the
      // validation of the 'z' control will be skipped.
      expect(validatorFn(form.get('z'))).toBeNull();
    });

    it('should error because of an empty value', () => {
      // Define prereqs that force the validator to validate the value of
      // the 'z' control.
      const validatorFn = CdValidators.requiredIf({
        x: true,
        y: 'abc'
      });
      // The validator must fail because the value of control 'z' is empty.
      expect(validatorFn(form.get('z'))).toEqual({ required: true });
    });

    it('should not error because of unsuccessful condition', () => {
      formHelper.setValue('z', 'zyx');
      // Define prereqs that force the validator to validate the value of
      // the 'z' control.
      const validatorFn = CdValidators.requiredIf(
        {
          x: true,
          z: 'zyx'
        },
        () => false
      );
      expect(validatorFn(form.get('z'))).toBeNull();
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
      expect(validatorFn(form.get('y'))).toEqual({ required: true });
    });
  });

  describe('custom validation', () => {
    beforeEach(() => {
      form = new CdFormGroup({
        x: new FormControl(3, CdValidators.custom('odd', (x) => x % 2 === 1)),
        y: new FormControl(
          5,
          CdValidators.custom('not-dividable-by-x', (y) => {
            const x = (form && form.get('x').value) || 1;
            return y % x !== 0;
          })
        )
      });
      formHelper = new FormHelper(form);
    });

    it('should test error and valid condition for odd x', () => {
      formHelper.expectError('x', 'odd');
      expectValid(4);
    });

    it('should test error and valid condition for y if its dividable by x', () => {
      updateValidity('y');
      formHelper.expectError('y', 'not-dividable-by-x');
      formHelper.expectValidChange('y', 6);
    });
  });

  describe('validate if condition', () => {
    beforeEach(() => {
      form = new CdFormGroup({
        x: new FormControl(3),
        y: new FormControl(5)
      });
      CdValidators.validateIf(form.get('x'), () => ((form && form.get('y').value) || 0) > 10, [
        CdValidators.custom('min', (x) => x < 7),
        CdValidators.custom('max', (x) => x > 12)
      ]);
      formHelper = new FormHelper(form);
    });

    it('should test min error', () => {
      formHelper.setValue('y', 11);
      updateValidity('x');
      formHelper.expectError('x', 'min');
    });

    it('should test max error', () => {
      formHelper.setValue('y', 11);
      formHelper.setValue('x', 13);
      formHelper.expectError('x', 'max');
    });

    it('should test valid number with validation', () => {
      formHelper.setValue('y', 11);
      formHelper.setValue('x', 12);
      formHelper.expectValid('x');
    });

    it('should validate automatically if dependency controls are defined', () => {
      CdValidators.validateIf(
        form.get('x'),
        () => ((form && form.getValue('y')) || 0) > 10,
        [Validators.min(7), Validators.max(12)],
        undefined,
        [form.get('y')]
      );

      formHelper.expectValid('x');
      formHelper.setValue('y', 13);
      formHelper.expectError('x', 'min');
    });

    it('should always validate the permanentValidators', () => {
      CdValidators.validateIf(
        form.get('x'),
        () => ((form && form.getValue('y')) || 0) > 10,
        [Validators.min(7), Validators.max(12)],
        [Validators.required],
        [form.get('y')]
      );

      formHelper.expectValid('x');
      formHelper.setValue('x', '');
      formHelper.expectError('x', 'required');
    });
  });

  describe('match', () => {
    let y: FormControl;

    beforeEach(() => {
      y = new FormControl('aaa');
      form = new CdFormGroup({
        x: new FormControl('aaa'),
        y: y
      });
      formHelper = new FormHelper(form);
    });

    it('should error when values are different', () => {
      formHelper.setValue('y', 'aab');
      CdValidators.match('x', 'y')(form);
      formHelper.expectValid('x');
      formHelper.expectError('y', 'match');
    });

    it('should not error when values are equal', () => {
      CdValidators.match('x', 'y')(form);
      formHelper.expectValid('x');
      formHelper.expectValid('y');
    });

    it('should unset error when values are equal', () => {
      y.setErrors({ match: true });
      CdValidators.match('x', 'y')(form);
      formHelper.expectValid('x');
      formHelper.expectValid('y');
    });

    it('should keep other existing errors', () => {
      y.setErrors({ match: true, notUnique: true });
      CdValidators.match('x', 'y')(form);
      formHelper.expectValid('x');
      formHelper.expectError('y', 'notUnique');
    });
  });

  describe('unique', () => {
    beforeEach(() => {
      form = new CdFormGroup({
        x: new FormControl(
          '',
          null,
          CdValidators.unique((value) => {
            return observableOf('xyz' === value);
          })
        )
      });
      formHelper = new FormHelper(form);
    });

    it('should not error because of empty input', () => {
      expectValid('');
    });

    it('should not error because of not existing input', fakeAsync(() => {
      formHelper.setValue('x', 'abc', true);
      tick(500);
      formHelper.expectValid('x');
    }));

    it('should error because of already existing input', fakeAsync(() => {
      formHelper.setValue('x', 'xyz', true);
      tick(500);
      formHelper.expectError('x', 'notUnique');
    }));
  });
});
