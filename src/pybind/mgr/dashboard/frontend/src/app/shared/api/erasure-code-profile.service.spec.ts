import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { ErasureCodeProfile } from '../models/erasure-code-profile';
import { ErasureCodeProfileService } from './erasure-code-profile.service';

describe('ErasureCodeProfileService', () => {
  let service: ErasureCodeProfileService;
  let httpTesting: HttpTestingController;
  const apiPath = 'api/erasure_code_profile';
  const testProfile: ErasureCodeProfile = { name: 'test', plugin: 'jerasure', k: 2, m: 1 };

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [ErasureCodeProfileService, i18nProviders]
  });

  beforeEach(() => {
    service = TestBed.inject(ErasureCodeProfileService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne(apiPath);
    expect(req.request.method).toBe('GET');
  });

  it('should call create', () => {
    service.create(testProfile).subscribe();
    const req = httpTesting.expectOne(apiPath);
    expect(req.request.method).toBe('POST');
  });

  it('should call delete', () => {
    service.delete('test').subscribe();
    const req = httpTesting.expectOne(`${apiPath}/test`);
    expect(req.request.method).toBe('DELETE');
  });

  it('should call getInfo', () => {
    service.getInfo().subscribe();
    const req = httpTesting.expectOne(`ui-${apiPath}/info`);
    expect(req.request.method).toBe('GET');
  });
});
