import { TestBed } from '@angular/core/testing';

import { CrudFormAdapterService } from './crud-form-adapter.service';
import { RouterTestingModule } from '@angular/router/testing';

describe('CrudFormAdapterService', () => {
  let service: CrudFormAdapterService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [RouterTestingModule]
    });
    service = TestBed.inject(CrudFormAdapterService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
