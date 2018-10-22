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
    service = TestBed.get(HostService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it(
    'should call list',
    fakeAsync(() => {
      let result;
      service.list().then((resp) => (result = resp));
      const req = httpTesting.expectOne('api/host');
      expect(req.request.method).toBe('GET');
      req.flush(['foo', 'bar']);
      tick();
      expect(result).toEqual(['foo', 'bar']);
    })
  );
});
