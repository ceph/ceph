import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { LogsService } from './logs.service';

describe('LogsService', () => {
  let service: LogsService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [LogsService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(LogsService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getLogs', () => {
    service.getLogs().subscribe();
    const req = httpTesting.expectOne('api/logs/all');
    expect(req.request.method).toBe('GET');
  });
});
