import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { OrchestratorService } from './orchestrator.service';

describe('OrchestratorService', () => {
  let service: OrchestratorService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [OrchestratorService, i18nProviders],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(OrchestratorService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call status', () => {
    service.status().subscribe();
    const req = httpTesting.expectOne(service.statusURL);
    expect(req.request.method).toBe('GET');
  });

  it('should call inventoryList', () => {
    service.inventoryList().subscribe();
    const req = httpTesting.expectOne(service.inventoryURL);
    expect(req.request.method).toBe('GET');
  });

  it('should call inventoryList with a host', () => {
    const host = 'host0';
    service.inventoryList(host).subscribe();
    const req = httpTesting.expectOne(`${service.inventoryURL}?hostname=${host}`);
    expect(req.request.method).toBe('GET');
  });

  it('should call serviceList', () => {
    service.serviceList().subscribe();
    const req = httpTesting.expectOne(service.serviceURL);
    expect(req.request.method).toBe('GET');
  });

  it('should call serviceList with a host', () => {
    const host = 'host0';
    service.serviceList(host).subscribe();
    const req = httpTesting.expectOne(`${service.serviceURL}?hostname=${host}`);
    expect(req.request.method).toBe('GET');
  });

  it('should call osdCreate', () => {
    const data = {
      drive_group: {
        host_pattern: '*'
      },
      all_hosts: ['a', 'b']
    };
    service.osdCreate(data['drive_group'], data['all_hosts']).subscribe();
    const req = httpTesting.expectOne(service.osdURL);
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(data);
  });
});
