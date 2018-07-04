import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { TcmuIscsiService } from './tcmu-iscsi.service';

describe('TcmuIscsiService', () => {
  let service: TcmuIscsiService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [TcmuIscsiService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(TcmuIscsiService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it(
    'should call tcmuiscsi',
    fakeAsync(() => {
      let result;
      service.tcmuiscsi().then((resp) => {
        result = resp;
      });
      const req = httpTesting.expectOne('api/tcmuiscsi');
      expect(req.request.method).toBe('GET');
      req.flush(['foo']);
      tick();
      expect(result).toEqual(['foo']);
    })
  );
});
