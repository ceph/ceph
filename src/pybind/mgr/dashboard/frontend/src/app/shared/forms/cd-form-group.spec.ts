import { AbstractControl, FormControl, FormGroup, NgForm } from '@angular/forms';

import { CdFormGroup } from './cd-form-group';

describe('CdFormGroup', () => {
  let form: CdFormGroup;

  const createTestForm = (controlName: string, value: any): FormGroup =>
    new FormGroup({
      [controlName]: new FormControl(value)
    });

  describe('test get and getValue in nested forms', () => {
    let formA: FormGroup;
    let formB: FormGroup;
    let formC: FormGroup;

    beforeEach(() => {
      formA = createTestForm('a', 'a');
      formB = createTestForm('b', 'b');
      formC = createTestForm('c', 'c');
      form = new CdFormGroup({
        formA: formA,
        formB: formB,
        formC: formC
      });
    });

    it('should find controls out of every form', () => {
      expect(form.get('a')).toBe(formA.get('a'));
      expect(form.get('b')).toBe(formB.get('b'));
      expect(form.get('c')).toBe(formC.get('c'));
    });

    it('should throw an error if element could be found', () => {
      expect(() => form.get('d')).toThrowError(`Control 'd' could not be found!`);
      expect(() => form.get('sth')).toThrowError(`Control 'sth' could not be found!`);
    });
  });

  describe('CdFormGroup tests', () => {
    let x, nested, a, c;

    beforeEach(() => {
      a = new FormControl('a');
      x = new CdFormGroup({
        a: a
      });
      nested = new CdFormGroup({
        lev1: new CdFormGroup({
          lev2: new FormControl('lev2')
        })
      });
      c = createTestForm('c', 'c');
      form = new CdFormGroup({
        nested: nested,
        cdform: x,
        b: new FormControl('b'),
        formC: c
      });
    });

    it('should return single value from "a" control in not nested form "x"', () => {
      expect(x.get('a')).toBe(a);
      expect(x.getValue('a')).toBe('a');
    });

    it('should return control "a" out of form "x" in nested form', () => {
      expect(form.get('a')).toBe(a);
      expect(form.getValue('a')).toBe('a');
    });

    it('should return value "b" that is not nested in nested form', () => {
      expect(form.getValue('b')).toBe('b');
    });

    it('return value "c" out of normal form group "c" in nested form', () => {
      expect(form.getValue('c')).toBe('c');
    });

    it('should return "lev2" value', () => {
      expect(form.getValue('lev2')).toBe('lev2');
    });

    it('should nested throw an error if control could not be found', () => {
      expect(() => form.get('d')).toThrowError(`Control 'd' could not be found!`);
      expect(() => form.getValue('sth')).toThrowError(`Control 'sth' could not be found!`);
    });
  });

  describe('test different values for getValue', () => {
    beforeEach(() => {
      form = new CdFormGroup({
        form_undefined: createTestForm('undefined', undefined),
        form_null: createTestForm('null', null),
        form_emptyObject: createTestForm('emptyObject', {}),
        form_filledObject: createTestForm('filledObject', { notEmpty: 1 }),
        form_number0: createTestForm('number0', 0),
        form_number1: createTestForm('number1', 1),
        form_emptyString: createTestForm('emptyString', ''),
        form_someString1: createTestForm('someString1', 's'),
        form_someString2: createTestForm('someString2', 'sth'),
        form_floating: createTestForm('floating', 0.1),
        form_false: createTestForm('false', false),
        form_true: createTestForm('true', true)
      });
    });

    it('returns objects', () => {
      expect(form.getValue('null')).toBe(null);
      expect(form.getValue('emptyObject')).toEqual({});
      expect(form.getValue('filledObject')).toEqual({ notEmpty: 1 });
    });

    it('returns set numbers', () => {
      expect(form.getValue('number0')).toBe(0);
      expect(form.getValue('number1')).toBe(1);
      expect(form.getValue('floating')).toBe(0.1);
    });

    it('returns strings', () => {
      expect(form.getValue('emptyString')).toBe('');
      expect(form.getValue('someString1')).toBe('s');
      expect(form.getValue('someString2')).toBe('sth');
    });

    it('returns booleans', () => {
      expect(form.getValue('true')).toBe(true);
      expect(form.getValue('false')).toBe(false);
    });

    it('returns null if control was set as undefined', () => {
      expect(form.getValue('undefined')).toBe(null);
    });

    it('returns a falsy value for null, undefined, false and 0', () => {
      expect(form.getValue('false')).toBeFalsy();
      expect(form.getValue('null')).toBeFalsy();
      expect(form.getValue('number0')).toBeFalsy();
    });
  });

  describe('should test showError', () => {
    let formDir: NgForm;
    let test: AbstractControl;

    beforeEach(() => {
      formDir = new NgForm([], []);
      form = new CdFormGroup({
        test_form: createTestForm('test', '')
      });
      test = form.get('test');
      test.setErrors({ someError: 'failed' });
    });

    it('should not show an error if not dirty and not submitted', () => {
      expect(form.showError('test', formDir)).toBe(false);
    });

    it('should show an error if dirty', () => {
      test.markAsDirty();
      expect(form.showError('test', formDir)).toBe(true);
    });

    it('should show an error if submitted', () => {
      expect(form.showError('test', <NgForm>{ submitted: true })).toBe(true);
    });

    it('should not show an error if no error exits', () => {
      test.setErrors(null);
      expect(form.showError('test', <NgForm>{ submitted: true })).toBe(false);
      test.markAsDirty();
      expect(form.showError('test', formDir)).toBe(false);
    });

    it('should not show error if the given error is not there', () => {
      expect(form.showError('test', <NgForm>{ submitted: true }, 'someOtherError')).toBe(false);
    });

    it('should show error if the given error is there', () => {
      expect(form.showError('test', <NgForm>{ submitted: true }, 'someError')).toBe(true);
    });
  });
});
