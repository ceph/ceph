import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { MonitorService } from './monitor.service';

describe('MonitorService', () => {
  let service: MonitorService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [MonitorService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(MonitorService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getMonitor', () => {
    service.getMonitor().subscribe();
    const req = httpTesting.expectOne('api/monitor');
    expect(req.request.method).toBe('GET');
  });
});
