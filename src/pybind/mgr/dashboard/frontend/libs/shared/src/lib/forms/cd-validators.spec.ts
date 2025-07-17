import { FormControl, FormGroup } from '@angular/forms';
import { CdValidators } from './cd-validators';
import { of } from 'rxjs';

describe('CdValidators', () => {
  describe('email', () => {
    it('should return null for empty value', () => {
      const control = new FormControl('');
      expect(CdValidators.email(control)).toBeNull();
    });
    it('should return error for invalid email', () => {
      const control = new FormControl('invalid');
      expect(CdValidators.email(control)).toEqual({ email: true });
    });
    it('should return null for valid email', () => {
      const control = new FormControl('test@example.com');
      expect(CdValidators.email(control)).toBeNull();
    });
  });

  describe('ip', () => {
    it('should validate IPv4', () => {
      const validator = CdValidators.ip(4);
      expect(validator(new FormControl('192.168.1.1'))).toBeNull();
      expect(validator(new FormControl('invalid'))).toEqual({ pattern: { requiredPattern: expect.any(String), actualValue: 'invalid' } });
    });
    it('should validate IPv6', () => {
      const validator = CdValidators.ip(6);
      expect(validator(new FormControl('2001:0db8:85a3:0000:0000:8a2e:0370:7334'))).toBeNull();
      expect(validator(new FormControl('invalid'))).toEqual({ pattern: { requiredPattern: expect.any(String), actualValue: 'invalid' } });
    });
    it('should validate both IPv4 and IPv6', () => {
      const validator = CdValidators.ip();
      expect(validator(new FormControl('192.168.1.1'))).toBeNull();
      expect(validator(new FormControl('2001:0db8:85a3:0000:0000:8a2e:0370:7334'))).toBeNull();
      expect(validator(new FormControl('invalid'))).toEqual({ pattern: { requiredPattern: expect.any(String), actualValue: 'invalid' } });
    });
  });

  describe('number', () => {
    it('should validate positive and negative numbers', () => {
      const validator = CdValidators.number();
      expect(validator(new FormControl('123'))).toBeNull();
      expect(validator(new FormControl('-123'))).toBeNull();
      expect(validator(new FormControl('abc'))).toEqual({ pattern: { requiredPattern: expect.any(String), actualValue: 'abc' } });
    });
    it('should validate only positive numbers', () => {
      const validator = CdValidators.number(false);
      expect(validator(new FormControl('123'))).toBeNull();
      expect(validator(new FormControl('-123'))).toEqual({ pattern: { requiredPattern: expect.any(String), actualValue: '-123' } });
    });
  });

  describe('decimalNumber', () => {
    it('should validate positive and negative decimals', () => {
      const validator = CdValidators.decimalNumber();
      expect(validator(new FormControl('123.45'))).toBeNull();
      expect(validator(new FormControl('-123.45'))).toBeNull();
      expect(validator(new FormControl('abc'))).toEqual({ pattern: { requiredPattern: expect.any(String), actualValue: 'abc' } });
    });
    it('should validate only positive decimals', () => {
      const validator = CdValidators.decimalNumber(false);
      expect(validator(new FormControl('123.45'))).toBeNull();
      expect(validator(new FormControl('-123.45'))).toEqual({ pattern: { requiredPattern: expect.any(String), actualValue: '-123.45' } });
    });
  });

  describe('sslCert', () => {
    it('should validate SSL certificate format', () => {
      const validator = CdValidators.sslCert();
      expect(validator(new FormControl('-----BEGIN CERTIFICATE-----\nabc\n-----END CERTIFICATE-----'))).toBeNull();
      expect(validator(new FormControl('invalid'))).toEqual({ pattern: { requiredPattern: expect.any(String), actualValue: 'invalid' } });
    });
  });

  describe('sslPrivKey', () => {
    it('should validate SSL private key format', () => {
      const validator = CdValidators.sslPrivKey();
      expect(validator(new FormControl('-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----'))).toBeNull();
      expect(validator(new FormControl('invalid'))).toEqual({ pattern: { requiredPattern: expect.any(String), actualValue: 'invalid' } });
    });
  });

  describe('pemCert', () => {
    it('should validate PEM certificate format', () => {
      const validator = CdValidators.pemCert();
      expect(validator(new FormControl('-----BEGIN TEST-----\nabc\n-----END TEST-----'))).toBeNull();
      expect(validator(new FormControl('invalid'))).toEqual({ pattern: { requiredPattern: expect.any(String), actualValue: 'invalid' } });
    });
  });

  describe('requiredIf', () => {
    it('should require value if prerequisite is met', () => {
      const group = new FormGroup({
        a: new FormControl(true),
        b: new FormControl('')
      });
      const validator = CdValidators.requiredIf({ a: true });
      group.get('b').setValidators(validator);
      group.get('b').updateValueAndValidity();
      expect(group.get('b').errors).toEqual({ required: true });
      group.get('b').setValue('value');
      group.get('b').updateValueAndValidity();
      expect(group.get('b').errors).toBeNull();
    });
  });

  describe('composeIf', () => {
    it('should compose validators if prerequisite is met', () => {
      const group = new FormGroup({
        a: new FormControl(true),
        b: new FormControl('')
      });
      const validator = CdValidators.composeIf({ a: true }, [CdValidators.email]);
      group.get('b').setValidators(validator);
      group.get('b').setValue('invalid');
      group.get('b').updateValueAndValidity();
      expect(group.get('b').errors).toEqual({ email: true });
    });
  });

  describe('custom', () => {
    it('should return custom error if condition is met', () => {
      const validator = CdValidators.custom('customError', (v: any) => v === 'bad');
      expect(validator(new FormControl('bad'))).toEqual({ customError: true });
      expect(validator(new FormControl('good'))).toBeNull();
    });
  });

  describe('uuid', () => {
    it('should validate UUIDs', () => {
      const validator = CdValidators.uuid();
      expect(validator(new FormControl('123e4567-e89b-12d3-a456-426614174000'))).toBeNull();
      expect(validator(new FormControl('invalid'))).toEqual({ invalidUuid: 'This is not a valid UUID' });
    });
  });

  describe('json', () => {
    it('should validate JSON', () => {
      const validator = CdValidators.json();
      expect(validator(new FormControl('{"a":1}'))).toBeNull();
      expect(validator(new FormControl('invalid'))).toEqual({ invalidJson: true });
    });
  });

  describe('xml', () => {
    it('should validate XML', () => {
      const validator = CdValidators.xml();
      expect(validator(new FormControl('<root></root>'))).toBeNull();
      expect(validator(new FormControl('<root>'))).toEqual({ invalidXml: true });
    });
  });

  describe('jsonOrXml', () => {
    it('should validate JSON or XML', () => {
      const validator = CdValidators.jsonOrXml();
      expect(validator(new FormControl('{"a":1}'))).toBeNull();
      expect(validator(new FormControl('<root></root>'))).toBeNull();
      expect(validator(new FormControl('invalid'))).toEqual({ invalidJson: true });
    });
  });

  describe('oauthAddressTest', () => {
    it('should validate oauth address', () => {
      const validator = CdValidators.oauthAddressTest();
      expect(validator(new FormControl('127.0.0.1:8080'))).toBeNull();
      expect(validator(new FormControl('invalid'))).toEqual({ invalidAddress: true });
    });
  });

  describe('url', () => {
    it('should validate URLs and IPs', () => {
      expect(CdValidators.url(new FormControl('http://example.com'))).toBeNull();
      expect(CdValidators.url(new FormControl('256.256.256.256'))).toEqual({ invalidURL: true });
      expect(CdValidators.url(new FormControl('invalid'))).toEqual({ invalidURL: true });
    });
  });

  // You can add more tests for async validators and other edge cases as needed.
});