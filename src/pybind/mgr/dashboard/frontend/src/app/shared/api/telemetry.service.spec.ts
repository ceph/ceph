import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { TelemetryService } from './telemetry.service';

describe('TelemetryService', () => {
  let service: TelemetryService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [TelemetryService]
  });

  beforeEach(() => {
    service = TestBed.inject(TelemetryService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getReport', () => {
    service.getReport().subscribe();
    const req = httpTesting.expectOne('api/telemetry/report');
    expect(req.request.method).toBe('GET');
  });

  it('should call enable to enable module', () => {
    service.enable(true).subscribe();
    const req = httpTesting.expectOne('api/telemetry');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body.enable).toBe(true);
    expect(req.request.body.license_name).toBe('sharing-1-0');
  });

  it('should call enable to disable module', () => {
    service.enable(false).subscribe();
    const req = httpTesting.expectOne('api/telemetry');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body.enable).toBe(false);
    expect(req.request.body.license_name).toBeUndefined();
  });

  it('should call enable to enable module by default', () => {
    service.enable().subscribe();
    const req = httpTesting.expectOne('api/telemetry');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body.enable).toBe(true);
    expect(req.request.body.license_name).toBe('sharing-1-0');
  });
});
