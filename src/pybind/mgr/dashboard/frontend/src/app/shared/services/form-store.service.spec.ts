import { TestBed } from '@angular/core/testing';

import { FormStoreService } from './form-store.service';
import { CdFormBuilder } from '../forms/cd-form-builder';
import { CdFormGroup } from '../forms/cd-form-group';
import { FormControl } from '@angular/forms';

describe('FormStoreService', () => {
  let service: FormStoreService;
  let formBuilder: CdFormBuilder;
  let form: CdFormGroup;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [FormStoreService]
    });

    service = TestBed.inject(FormStoreService);
    formBuilder = TestBed.inject(CdFormBuilder);

    form = formBuilder.group({
      foo: new FormControl(['Test']),
      bar: new FormControl(30),
      data: new FormControl([
        [1, 2, 3],
        ['a', 'b', 'c'],
        [4, 'd']
      ]),
      custom: new FormControl([])
    });
  });

  it('should save and load form values', () => {
    service.save(form);

    const loaded = service.load();
    expect(loaded).toEqual({
      foo: ['Test'],
      bar: 30,
      data: [
        [1, 2, 3],
        ['a', 'b', 'c'],
        [4, 'd']
      ],
      custom: []
    });
  });

  it('should return null if nothing is saved', () => {
    expect(service.load()).toBeNull();
  });

  it('should clear saved form values when clearFormData', () => {
    service.save(form);
    service.clearFormData = true;
    service.clear();
    expect(service.load()).toBeNull();
  });

  it('should update saved values when form changes', () => {
    service.save(form);

    form.patchValue({ foo: 'Admin', bar: 1, data: [[10], ['x', 'y']], custom: [1] });
    service.save(form);

    expect(service.load()).toEqual({ foo: 'Admin', bar: 1, data: [[10], ['x', 'y']], custom: [1] });
  });
});
