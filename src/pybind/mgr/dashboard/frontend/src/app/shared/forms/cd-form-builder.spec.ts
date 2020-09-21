import { Validators } from '@angular/forms';

import { CdFormBuilder } from './cd-form-builder';
import { CdFormGroup } from './cd-form-group';

describe('cd-form-builder', () => {
  let service: CdFormBuilder;

  beforeEach(() => {
    service = new CdFormBuilder();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should create a nested CdFormGroup', () => {
    const form = service.group({
      nested: service.group({
        a: [null],
        b: ['sth'],
        c: [2, [Validators.min(3)]]
      }),
      d: [{ e: 3 }],
      f: [true]
    });
    expect(form.constructor).toBe(CdFormGroup);
    expect(form instanceof CdFormGroup).toBeTruthy();
    expect(form.getValue('b')).toBe('sth');
    expect(form.getValue('d')).toEqual({ e: 3 });
    expect(form.get('c').valid).toBeFalsy();
  });
});
