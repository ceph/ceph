import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { DashboardService } from './dashboard.service';

describe('DashboardService', () => {
  let service: DashboardService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [DashboardService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(DashboardService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getHealth', () => {
    service.getHealth().subscribe();
    const req = httpTesting.expectOne('api/dashboard/health');
    expect(req.request.method).toBe('GET');
  });
});
