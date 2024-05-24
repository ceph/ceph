import { fakeAsync, tick } from '@angular/core/testing';
import { FormControl, Validators } from '@angular/forms';

import _ from 'lodash';
import { of as observableOf } from 'rxjs';

import { RgwBucketService } from '~/app/shared/api/rgw-bucket.service';
import { CdFormGroup } from '~/app/shared/forms/cd-form-group';
import { CdValidators, DUE_TIMER } from '~/app/shared/forms/cd-validators';
import { FormHelper } from '~/testing/unit-test-helper';

let mockBucketExists = observableOf(true);
jest.mock('~/app/shared/api/rgw-bucket.service', () => {
  return {
    RgwBucketService: jest.fn().mockImplementation(() => {
      return {
        exists: () => mockBucketExists
      };
    })
  };
});

describe('CdValidators', () => {
  let formHelper: FormHelper;
  let form: CdFormGroup;

  const expectValid = (value: any) => formHelper.expectValidChange('x', value);
  const expectPatternError = (value: any) => formHelper.expectErrorChange('x', value, 'pattern');
  const updateValidity = (controlName: string) => form.get(controlName).updateValueAndValidity();

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
    const expectUuidError = (value: string) =>
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
        a: new FormControl(''),
        b: new FormControl('xyz'),
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
      const conditionFn = (value: string) => {
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

    it('should process extended prerequisites (1)', () => {
      const validatorFn = CdValidators.requiredIf({
        y: { op: '!empty' }
      });
      expect(validatorFn(form.get('z'))).toEqual({ required: true });
    });

    it('should process extended prerequisites (2)', () => {
      const validatorFn = CdValidators.requiredIf({
        y: { op: '!empty' }
      });
      expect(validatorFn(form.get('b'))).toBeNull();
    });

    it('should process extended prerequisites (3)', () => {
      const validatorFn = CdValidators.requiredIf({
        y: { op: 'minLength', arg1: 2 }
      });
      expect(validatorFn(form.get('z'))).toEqual({ required: true });
    });

    it('should process extended prerequisites (4)', () => {
      const validatorFn = CdValidators.requiredIf({
        z: { op: 'empty' }
      });
      expect(validatorFn(form.get('a'))).toEqual({ required: true });
    });

    it('should process extended prerequisites (5)', () => {
      const validatorFn = CdValidators.requiredIf({
        z: { op: 'empty' }
      });
      expect(validatorFn(form.get('y'))).toBeNull();
    });

    it('should process extended prerequisites (6)', () => {
      const validatorFn = CdValidators.requiredIf({
        y: { op: 'empty' }
      });
      expect(validatorFn(form.get('z'))).toBeNull();
    });

    it('should process extended prerequisites (7)', () => {
      const validatorFn = CdValidators.requiredIf({
        y: { op: 'minLength', arg1: 4 }
      });
      expect(validatorFn(form.get('z'))).toBeNull();
    });

    it('should process extended prerequisites (8)', () => {
      const validatorFn = CdValidators.requiredIf({
        x: { op: 'equal', arg1: true }
      });
      expect(validatorFn(form.get('z'))).toEqual({ required: true });
    });

    it('should process extended prerequisites (9)', () => {
      const validatorFn = CdValidators.requiredIf({
        b: { op: '!equal', arg1: 'abc' }
      });
      expect(validatorFn(form.get('z'))).toEqual({ required: true });
    });
  });

  describe('custom validation', () => {
    beforeEach(() => {
      form = new CdFormGroup({
        x: new FormControl(
          3,
          CdValidators.custom('odd', (x: number) => x % 2 === 1)
        ),
        y: new FormControl(
          5,
          CdValidators.custom('not-dividable-by-x', (y: number) => {
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
        CdValidators.custom('min', (x: number) => x < 7),
        CdValidators.custom('max', (x: number) => x > 12)
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

  describe('composeIf', () => {
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
      const validatorFn = CdValidators.composeIf(
        {
          x: true,
          y: 'abc'
        },
        [Validators.required]
      );
      expect(validatorFn(form.get('z'))).toBeNull();
    });

    it('should not error because of unmet prerequisites', () => {
      // Define prereqs that do not match the current values of the form fields.
      const validatorFn = CdValidators.composeIf(
        {
          x: false,
          y: 'xyz'
        },
        [Validators.required]
      );
      // The validator must succeed because the prereqs do not match, so the
      // validation of the 'z' control will be skipped.
      expect(validatorFn(form.get('z'))).toBeNull();
    });

    it('should error because of an empty value', () => {
      // Define prereqs that force the validator to validate the value of
      // the 'z' control.
      const validatorFn = CdValidators.composeIf(
        {
          x: true,
          y: 'abc'
        },
        [Validators.required]
      );
      // The validator must fail because the value of control 'z' is empty.
      expect(validatorFn(form.get('z'))).toEqual({ required: true });
    });
  });

  describe('dimlessBinary validators', () => {
    const i18nMock = (a: string, b: { value: string }) => a.replace('{{value}}', b.value);

    beforeEach(() => {
      form = new CdFormGroup({
        x: new FormControl('2 KiB', [CdValidators.binaryMin(1024), CdValidators.binaryMax(3072)])
      });
      formHelper = new FormHelper(form);
    });

    it('should not raise exception an exception for valid change', () => {
      formHelper.expectValidChange('x', '2.5 KiB');
    });

    it('should not raise minimum error', () => {
      formHelper.expectErrorChange('x', '0.5 KiB', 'binaryMin');
      expect(form.get('x').getError('binaryMin')(i18nMock)).toBe(
        'Size has to be at least 1 KiB or more'
      );
    });

    it('should not raise maximum error', () => {
      formHelper.expectErrorChange('x', '4 KiB', 'binaryMax');
      expect(form.get('x').getError('binaryMax')(i18nMock)).toBe(
        'Size has to be at most 3 KiB or less'
      );
    });
  });

  describe('passwordPolicy', () => {
    let valid: boolean;
    let callbackCalled: boolean;

    const fakeUserService = {
      validatePassword: () => {
        return observableOf({ valid: valid, credits: 17, valuation: 'foo' });
      }
    };

    beforeEach(() => {
      callbackCalled = false;
      form = new CdFormGroup({
        x: new FormControl(
          '',
          null,
          CdValidators.passwordPolicy(
            fakeUserService,
            () => 'admin',
            () => {
              callbackCalled = true;
            }
          )
        )
      });
      formHelper = new FormHelper(form);
    });

    it('should not error because of empty input', () => {
      expectValid('');
      expect(callbackCalled).toBeTruthy();
    });

    it('should not error because password matches the policy', fakeAsync(() => {
      valid = true;
      formHelper.setValue('x', 'abc', true);
      tick(500);
      formHelper.expectValid('x');
    }));

    it('should error because password does not match the policy', fakeAsync(() => {
      valid = false;
      formHelper.setValue('x', 'xyz', true);
      tick(500);
      formHelper.expectError('x', 'passwordPolicy');
    }));

    it('should call the callback function', fakeAsync(() => {
      formHelper.setValue('x', 'xyz', true);
      tick(500);
      expect(callbackCalled).toBeTruthy();
    }));

    describe('sslCert validator', () => {
      beforeEach(() => {
        form.get('x').setValidators(CdValidators.sslCert());
      });

      it('should not error because of empty input', () => {
        expectValid('');
      });

      it('should accept SSL certificate', () => {
        expectValid(
          '-----BEGIN CERTIFICATE-----\n' +
            'MIIC1TCCAb2gAwIBAgIJAM33ZCMvOLVdMA0GCSqGSIb3DQEBBQUAMBoxGDAWBgNV\n' +
            '...\n' +
            '3Ztorm2A5tFB\n' +
            '-----END CERTIFICATE-----\n' +
            '\n'
        );
      });

      it('should error on invalid SSL certificate (1)', () => {
        expectPatternError(
          'MIIC1TCCAb2gAwIBAgIJAM33ZCMvOLVdMA0GCSqGSIb3DQEBBQUAMBoxGDAWBgNV\n' +
            '...\n' +
            '3Ztorm2A5tFB\n' +
            '-----END CERTIFICATE-----\n' +
            '\n'
        );
      });

      it('should error on invalid SSL certificate (2)', () => {
        expectPatternError(
          '-----BEGIN CERTIFICATE-----\n' +
            'MIIC1TCCAb2gAwIBAgIJAM33ZCMvOLVdMA0GCSqGSIb3DQEBBQUAMBoxGDAWBgNV\n'
        );
      });
    });

    describe('sslPrivKey validator', () => {
      beforeEach(() => {
        form.get('x').setValidators(CdValidators.sslPrivKey());
      });

      it('should not error because of empty input', () => {
        expectValid('');
      });

      it('should accept SSL private key', () => {
        expectValid(
          '-----BEGIN RSA PRIVATE KEY-----\n' +
            'MIIEpQIBAAKCAQEA5VwkMK63D7AoGJVbVpgiV3XlEC1rwwOEpHPZW9F3ZW1fYS1O\n' +
            '...\n' +
            'SA4Jbana77S7adg919vNBCLWPAeoN44lI2+B1Ub5DxSnOpBf+zKiScU=\n' +
            '-----END RSA PRIVATE KEY-----\n' +
            '\n'
        );
      });

      it('should error on invalid SSL private key (1)', () => {
        expectPatternError(
          'MIIEpQIBAAKCAQEA5VwkMK63D7AoGJVbVpgiV3XlEC1rwwOEpHPZW9F3ZW1fYS1O\n' +
            '...\n' +
            'SA4Jbana77S7adg919vNBCLWPAeoN44lI2+B1Ub5DxSnOpBf+zKiScU=\n' +
            '-----END RSA PRIVATE KEY-----\n' +
            '\n'
        );
      });

      it('should error on invalid SSL private key (2)', () => {
        expectPatternError(
          '-----BEGIN RSA PRIVATE KEY-----\n' +
            'MIIEpQIBAAKCAQEA5VwkMK63D7AoGJVbVpgiV3XlEC1rwwOEpHPZW9F3ZW1fYS1O\n' +
            '...\n' +
            'SA4Jbana77S7adg919vNBCLWPAeoN44lI2+B1Ub5DxSnOpBf+zKiScU=\n'
        );
      });
    });
  });
  describe('bucket', () => {
    const testValidator = (name: string, valid: boolean, expectedError?: string) => {
      formHelper.setValue('x', name, true);
      tick(DUE_TIMER);
      if (valid) {
        formHelper.expectValid('x');
      } else {
        formHelper.expectError('x', expectedError);
      }
    };

    describe('bucketName', () => {
      beforeEach(() => {
        form = new CdFormGroup({
          x: new FormControl('', null, CdValidators.bucketName())
        });
        formHelper = new FormHelper(form);
      });

      it('bucket name cannot be empty', fakeAsync(() => {
        testValidator('', false, 'required');
      }));

      it('bucket names cannot be formatted as IP address', fakeAsync(() => {
        const testIPs = ['1.1.1.01', '001.1.1.01', '127.0.0.1'];
        for (const ip of testIPs) {
          testValidator(ip, false, 'ipAddress');
        }
      }));

      it('bucket name must be >= 3 characters long (1/2)', fakeAsync(() => {
        testValidator('ab', false, 'shouldBeInRange');
      }));

      it('bucket name must be >= 3 characters long (2/2)', fakeAsync(() => {
        testValidator('abc', true);
      }));

      it('bucket name must be <= than 63 characters long (1/2)', fakeAsync(() => {
        testValidator(_.repeat('a', 64), false, 'shouldBeInRange');
      }));

      it('bucket name must be <= than 63 characters long (2/2)', fakeAsync(() => {
        testValidator(_.repeat('a', 63), true);
      }));

      it('bucket names must not contain uppercase characters or underscores (1/2)', fakeAsync(() => {
        testValidator('iAmInvalid', false, 'bucketNameInvalid');
      }));

      it('bucket names can only contain lowercase letters, numbers, periods and hyphens', fakeAsync(() => {
        testValidator('bk@2', false, 'bucketNameInvalid');
      }));

      it('bucket names must not contain uppercase characters or underscores (2/2)', fakeAsync(() => {
        testValidator('i_am_invalid', false, 'bucketNameInvalid');
      }));

      it('bucket names must start and end with letters or numbers', fakeAsync(() => {
        testValidator('abcd-', false, 'lowerCaseOrNumber');
      }));

      it('bucket labels cannot be empty', fakeAsync(() => {
        testValidator('bk.', false, 'onlyLowerCaseAndNumbers');
      }));

      it('bucket names with invalid labels (1/3)', fakeAsync(() => {
        testValidator('abc.1def.Ghi2', false, 'bucketNameInvalid');
      }));

      it('bucket names with invalid labels (2/3)', fakeAsync(() => {
        testValidator('abc.1_xy', false, 'bucketNameInvalid');
      }));

      it('bucket names with invalid labels (3/3)', fakeAsync(() => {
        testValidator('abc.*def', false, 'bucketNameInvalid');
      }));

      it('bucket names must be a series of one or more labels and can contain lowercase letters, numbers, and hyphens (1/3)', fakeAsync(() => {
        testValidator('xyz.abc', true);
      }));

      it('bucket names must be a series of one or more labels and can contain lowercase letters, numbers, and hyphens (2/3)', fakeAsync(() => {
        testValidator('abc.1-def', true);
      }));

      it('bucket names must be a series of one or more labels and can contain lowercase letters, numbers, and hyphens (3/3)', fakeAsync(() => {
        testValidator('abc.ghi2', true);
      }));

      it('bucket names must be unique', fakeAsync(() => {
        testValidator('bucket-name-is-unique', true);
      }));

      it('bucket names must not contain spaces', fakeAsync(() => {
        testValidator('bucket name  with   spaces', false, 'bucketNameInvalid');
      }));
    });

    describe('bucketExistence', () => {
      const rgwBucketService = new RgwBucketService(undefined, undefined);

      beforeEach(() => {
        form = new CdFormGroup({
          x: new FormControl('', null, CdValidators.bucketExistence(false, rgwBucketService))
        });
        formHelper = new FormHelper(form);
      });

      it('bucket name cannot be empty', fakeAsync(() => {
        testValidator('', false, 'required');
      }));

      it('bucket name should not exist but it does', fakeAsync(() => {
        testValidator('testName', false, 'bucketNameNotAllowed');
      }));

      it('bucket name should not exist and it does not', fakeAsync(() => {
        mockBucketExists = observableOf(false);
        testValidator('testName', true);
      }));

      it('bucket name should exist but it does not', fakeAsync(() => {
        form.get('x').setAsyncValidators(CdValidators.bucketExistence(true, rgwBucketService));
        mockBucketExists = observableOf(false);
        testValidator('testName', false, 'bucketNameNotAllowed');
      }));

      it('bucket name should exist and it does', fakeAsync(() => {
        form.get('x').setAsyncValidators(CdValidators.bucketExistence(true, rgwBucketService));
        mockBucketExists = observableOf(true);
        testValidator('testName', true);
      }));
    });
  });
});
