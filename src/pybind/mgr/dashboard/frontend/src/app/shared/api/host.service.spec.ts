import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { HostService } from './host.service';

describe('HostService', () => {
  let service: HostService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [HostService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(HostService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list', fakeAsync(() => {
    let result;
    service.list().subscribe((resp) => (result = resp));
    const req = httpTesting.expectOne('api/host');
    expect(req.request.method).toBe('GET');
    req.flush(['foo', 'bar']);
    tick();
    expect(result).toEqual(['foo', 'bar']);
  }));

  it('should make a GET request on the devices endpoint when requesting devices', () => {
    const hostname = 'hostname';
    service.getDevices(hostname).subscribe();
    const req = httpTesting.expectOne(`api/host/${hostname}/devices`);
    expect(req.request.method).toBe('GET');
  });
});
