import { TestBed } from '@angular/core/testing';

import { CrudFormAdapterService } from './crud-form-adapter.service';

describe('CrudFormAdapterService', () => {
  let service: CrudFormAdapterService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(CrudFormAdapterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
