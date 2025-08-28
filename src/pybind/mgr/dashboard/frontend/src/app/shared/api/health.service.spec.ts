import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
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

  it('should call getHealthSnapshot', () => {
    service.getHealthSnapshot().subscribe();
    const req = httpTesting.expectOne('api/health/snapshot');
    expect(req.request.method).toBe('GET');
  });

  it('should call getClusterFsid', () => {
    service.getClusterFsid().subscribe();
    const req = httpTesting.expectOne('api/health/get_cluster_fsid');
    expect(req.request.method).toBe('GET');
  });

  it('should call getOrchestratorName', () => {
    service.getOrchestratorName().subscribe();
    const req = httpTesting.expectOne('api/health/get_orchestrator_name');
    expect(req.request.method).toBe('GET');
  });

  it('should call getTelemetryStatus', () => {
    service.getTelemetryStatus().subscribe();
    const req = httpTesting.expectOne('api/health/get_telemetry_status');
    expect(req.request.method).toBe('GET');
  });
});
