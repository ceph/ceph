import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { CephfsService } from './cephfs.service';

describe('CephfsService', () => {
  let service: CephfsService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [CephfsService]
  });

  beforeEach(() => {
    service = TestBed.inject(CephfsService);
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

  it('should call getTabs', () => {
    service.getTabs(2).subscribe();
    const req = httpTesting.expectOne('ui-api/cephfs/2/tabs');
    expect(req.request.method).toBe('GET');
  });

  it('should call getMdsCounters', () => {
    service.getMdsCounters('1').subscribe();
    const req = httpTesting.expectOne('api/cephfs/1/mds_counters');
    expect(req.request.method).toBe('GET');
  });

  it('should call lsDir', () => {
    service.lsDir(1).subscribe();
    const req = httpTesting.expectOne('ui-api/cephfs/1/ls_dir?depth=2');
    expect(req.request.method).toBe('GET');
    service.lsDir(2, '/some/path').subscribe();
    httpTesting.expectOne('ui-api/cephfs/2/ls_dir?depth=2&path=%252Fsome%252Fpath');
  });

  it('should call mkSnapshot', () => {
    service.mkSnapshot(3, '/some/path').subscribe();
    const req = httpTesting.expectOne('api/cephfs/3/mk_snapshot?path=%252Fsome%252Fpath');
    expect(req.request.method).toBe('POST');

    service.mkSnapshot(4, '/some/other/path', 'snap').subscribe();
    httpTesting.expectOne('api/cephfs/4/mk_snapshot?path=%252Fsome%252Fother%252Fpath&name=snap');
  });

  it('should call rmSnapshot', () => {
    service.rmSnapshot(1, '/some/path', 'snap').subscribe();
    const req = httpTesting.expectOne('api/cephfs/1/rm_snapshot?path=%252Fsome%252Fpath&name=snap');
    expect(req.request.method).toBe('POST');
  });

  it('should call updateQuota', () => {
    service.updateQuota(1, '/some/path', { max_bytes: 1024 }).subscribe();
    let req = httpTesting.expectOne('api/cephfs/1/set_quotas?path=%252Fsome%252Fpath');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ max_bytes: 1024 });

    service.updateQuota(1, '/some/path', { max_files: 10 }).subscribe();
    req = httpTesting.expectOne('api/cephfs/1/set_quotas?path=%252Fsome%252Fpath');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ max_files: 10 });

    service.updateQuota(1, '/some/path', { max_bytes: 1024, max_files: 10 }).subscribe();
    req = httpTesting.expectOne('api/cephfs/1/set_quotas?path=%252Fsome%252Fpath');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ max_bytes: 1024, max_files: 10 });
  });
});
