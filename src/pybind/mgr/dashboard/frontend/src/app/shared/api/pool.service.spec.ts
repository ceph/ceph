import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { PoolService } from './pool.service';

describe('PoolService', () => {
  let service: PoolService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [PoolService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(PoolService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getList', () => {
    service.getList().subscribe();
    const req = httpTesting.expectOne('api/pool');
    expect(req.request.method).toBe('GET');
  });

  it(
    'should call list without parameter',
    fakeAsync(() => {
      let result;
      service.list().then((resp) => (result = resp));
      const req = httpTesting.expectOne('api/pool?attrs=');
      expect(req.request.method).toBe('GET');
      req.flush(['foo', 'bar']);
      tick();
      expect(result).toEqual(['foo', 'bar']);
    })
  );

  it(
    'should call list with a list',
    fakeAsync(() => {
      let result;
      service.list(['foo']).then((resp) => (result = resp));
      const req = httpTesting.expectOne('api/pool?attrs=foo');
      expect(req.request.method).toBe('GET');
      req.flush(['foo', 'bar']);
      tick();
      expect(result).toEqual(['foo', 'bar']);
    })
  );
});
