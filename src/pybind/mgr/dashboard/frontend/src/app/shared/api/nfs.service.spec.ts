import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { fakeAsync, TestBed, tick } from '@angular/core/testing';

import { configureTestBed } from '~/testing/unit-test-helper';
import { NfsService } from './nfs.service';

describe('NfsService', () => {
  let service: NfsService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [NfsService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.inject(NfsService);
    httpTesting = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list', () => {
    let cluster_id = 'test';
    service.list(cluster_id).subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/export?cluster_id=test');
    expect(req.request.method).toBe('GET');
  });

  it('should call get', () => {
    service.get('cluster_id', 'export_id').subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/export/cluster_id/export_id');
    expect(req.request.method).toBe('GET');
  });

  it('should call create', () => {
    service.create('foo').subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/export');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual('foo');
  });

  it('should call update', () => {
    service.update('cluster_id', 1, 'foo').subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/export/cluster_id/1');
    expect(req.request.body).toEqual('foo');
    expect(req.request.method).toBe('PUT');
  });

  it('should call delete', () => {
    service.delete('hostName', 'exportId').subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/export/hostName/exportId');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call lsDir', () => {
    service.lsDir('a', 'foo_dir').subscribe();
    const req = httpTesting.expectOne('ui-api/nfs-ganesha/lsdir/a?root_dir=foo_dir');
    expect(req.request.method).toBe('GET');
  });

  it('should not call lsDir if volume is not provided', fakeAsync(() => {
    service.lsDir('', 'foo_dir').subscribe({
      error: (error: string) => expect(error).toEqual('Please specify a filesystem volume.')
    });
    tick();
    httpTesting.expectNone('ui-api/nfs-ganesha/lsdir/?root_dir=foo_dir');
  }));
});
