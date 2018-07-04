import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { LoggingService } from './logging.service';

describe('LoggingService', () => {
  let service: LoggingService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [LoggingService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(LoggingService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call jsError', () => {
    service.jsError('foo', 'bar', 'baz').subscribe();
    const req = httpTesting.expectOne('ui-api/logging/js-error');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({
      url: 'foo',
      message: 'bar',
      stack: 'baz'
    });
  });
});
