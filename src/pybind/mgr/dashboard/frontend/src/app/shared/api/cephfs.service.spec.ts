import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { CephfsService } from './cephfs.service';

describe('CephfsService', () => {
  let service: CephfsService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [CephfsService]
  });

  beforeEach(() => {
    service = TestBed.get(CephfsService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne('api/cephfs');
    expect(req.request.method).toBe('GET');
  });

  it('should call getCephfs', () => {
    service.getCephfs(1).subscribe();
    const req = httpTesting.expectOne('api/cephfs/1');
    expect(req.request.method).toBe('GET');
  });

  it('should call getClients', () => {
    service.getClients(1).subscribe();
    const req = httpTesting.expectOne('api/cephfs/1/clients');
    expect(req.request.method).toBe('GET');
  });

  it('should call getMdsCounters', () => {
    service.getMdsCounters(1).subscribe();
    const req = httpTesting.expectOne('api/cephfs/1/mds_counters');
    expect(req.request.method).toBe('GET');
  });
});
