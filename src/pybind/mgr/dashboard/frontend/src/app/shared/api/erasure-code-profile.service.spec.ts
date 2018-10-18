import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { ErasureCodeProfileService } from './erasure-code-profile.service';

describe('ErasureCodeProfileService', () => {
  let service: ErasureCodeProfileService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [ErasureCodeProfileService]
  });

  beforeEach(() => {
    service = TestBed.get(ErasureCodeProfileService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne('api/erasure_code_profile');
    expect(req.request.method).toBe('GET');
  });
});
