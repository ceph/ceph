import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { NfsService } from './nfs.service';

describe('NfsService', () => {
  let service: NfsService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [NfsService, i18nProviders],
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
    service.list().subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/export');
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
    service.update('cluster_id', 'export_id', 'foo').subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/export/cluster_id/export_id');
    expect(req.request.body).toEqual('foo');
    expect(req.request.method).toBe('PUT');
  });

  it('should call delete', () => {
    service.delete('hostName', 'exportId').subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/export/hostName/exportId');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call lsDir', () => {
    service.lsDir('foo_dir').subscribe();
    const req = httpTesting.expectOne('ui-api/nfs-ganesha/lsdir?root_dir=foo_dir');
    expect(req.request.method).toBe('GET');
  });

  it('should call buckets', () => {
    service.buckets('user_foo').subscribe();
    const req = httpTesting.expectOne('ui-api/nfs-ganesha/rgw/buckets?user_id=user_foo');
    expect(req.request.method).toBe('GET');
  });

  it('should call daemon', () => {
    service.daemon().subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/daemon');
    expect(req.request.method).toBe('GET');
  });

  it('should call start', () => {
    service.start('host_name').subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/service/host_name/start');
    expect(req.request.method).toBe('PUT');
  });

  it('should call stop', () => {
    service.stop('host_name').subscribe();
    const req = httpTesting.expectOne('api/nfs-ganesha/service/host_name/stop');
    expect(req.request.method).toBe('PUT');
  });
});
