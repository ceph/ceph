import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { HealthService } from './health.service';

describe('HealthService', () => {
  let service: HealthService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [HealthService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(HealthService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getFullHealth', () => {
    service.getFullHealth().subscribe();
    const req = httpTesting.expectOne('api/health/full');
    expect(req.request.method).toBe('GET');
  });

  it('should call getMinimalHealth', () => {
    service.getMinimalHealth().subscribe();
    const req = httpTesting.expectOne('api/health/minimal');
    expect(req.request.method).toBe('GET');
  });
});
