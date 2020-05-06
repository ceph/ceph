import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { ScopeService } from './scope.service';

describe('ScopeService', () => {
  let service: ScopeService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [ScopeService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(ScopeService);
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
    const req = httpTesting.expectOne('ui-api/scope');
    expect(req.request.method).toBe('GET');
  });
});
