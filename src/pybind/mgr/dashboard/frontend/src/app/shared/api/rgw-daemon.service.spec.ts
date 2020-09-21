import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { RgwDaemonService } from './rgw-daemon.service';

describe('RgwDaemonService', () => {
  let service: RgwDaemonService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [RgwDaemonService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(RgwDaemonService);
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
    const req = httpTesting.expectOne('api/rgw/daemon');
    expect(req.request.method).toBe('GET');
  });

  it('should call get', () => {
    service.get('foo').subscribe();
    const req = httpTesting.expectOne('api/rgw/daemon/foo');
    expect(req.request.method).toBe('GET');
  });
});
