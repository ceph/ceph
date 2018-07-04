import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { RbdMirroringService } from './rbd-mirroring.service';

describe('RbdMirroringService', () => {
  let service: RbdMirroringService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [RbdMirroringService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(RbdMirroringService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call get', () => {
    service.get().subscribe();
    const req = httpTesting.expectOne('api/rbdmirror');
    expect(req.request.method).toBe('GET');
  });
});
