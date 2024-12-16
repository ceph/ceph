import { TestBed } from '@angular/core/testing';
import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';

import { SmbService } from './smb.service';
import { configureTestBed } from '~/testing/unit-test-helper';

describe('SmbService', () => {
  let service: SmbService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [SmbService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(SmbService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list', () => {
    service.listClusters().subscribe();
    const req = httpTesting.expectOne('api/smb/cluster');
    expect(req.request.method).toBe('GET');
  });

  it('should call create', () => {
    service.createCluster('test').subscribe();
    const req = httpTesting.expectOne('api/smb/cluster');
    expect(req.request.method).toBe('POST');
  });

  it('should call remove', () => {
    service.removeCluster('cluster_1').subscribe();
    const req = httpTesting.expectOne('api/smb/cluster/cluster_1');
    expect(req.request.method).toBe('DELETE');

  });

  it('should call list shares for a given cluster', () => {
    service.listShares('tango').subscribe();
    const req = httpTesting.expectOne('api/smb/share?cluster_id=tango');
    expect(req.request.method).toBe('GET');
  });
});
