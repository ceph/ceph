import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { PerformanceCounterService } from './performance-counter.service';

describe('PerformanceCounterService', () => {
  let service: PerformanceCounterService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [PerformanceCounterService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(PerformanceCounterService);
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
    const req = httpTesting.expectOne('api/perf_counters');
    expect(req.request.method).toBe('GET');
  });

  it('should call get', () => {
    let result;
    service.get('foo', '1').subscribe((resp) => {
      result = resp;
    });
    const req = httpTesting.expectOne('api/perf_counters/foo/1');
    expect(req.request.method).toBe('GET');
    req.flush({ counters: [{ foo: 'bar' }] });
    expect(result).toEqual([{ foo: 'bar' }]);
  });
});
