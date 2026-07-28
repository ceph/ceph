import { configureTestBed } from '~/testing/unit-test-helper';
import { TestBed } from '@angular/core/testing';

import { CrudFormAdapterService } from './crud-form-adapter.service';
import { RouterTestingModule } from '@angular/router/testing';

describe('CrudFormAdapterService', () => {
  let service: CrudFormAdapterService;

  configureTestBed({
    imports: [RouterTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(CrudFormAdapterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
