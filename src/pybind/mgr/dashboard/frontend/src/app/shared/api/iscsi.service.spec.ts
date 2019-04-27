import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { IscsiService } from './iscsi.service';

describe('IscsiService', () => {
  let service: IscsiService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [IscsiService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(IscsiService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call listTargets', () => {
    service.listTargets().subscribe();
    const req = httpTesting.expectOne('api/iscsi/target');
    expect(req.request.method).toBe('GET');
  });

  it('should call getTarget', () => {
    service.getTarget('iqn.foo').subscribe();
    const req = httpTesting.expectOne('api/iscsi/target/iqn.foo');
    expect(req.request.method).toBe('GET');
  });

  it('should call status', () => {
    service.status().subscribe();
    const req = httpTesting.expectOne('ui-api/iscsi/status');
    expect(req.request.method).toBe('GET');
  });

  it('should call settings', () => {
    service.settings().subscribe();
    const req = httpTesting.expectOne('ui-api/iscsi/settings');
    expect(req.request.method).toBe('GET');
  });

  it('should call portals', () => {
    service.portals().subscribe();
    const req = httpTesting.expectOne('ui-api/iscsi/portals');
    expect(req.request.method).toBe('GET');
  });

  it('should call createTarget', () => {
    service.createTarget({ target_iqn: 'foo' }).subscribe();
    const req = httpTesting.expectOne('api/iscsi/target');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ target_iqn: 'foo' });
  });

  it('should call updateTarget', () => {
    service.updateTarget('iqn.foo', { target_iqn: 'foo' }).subscribe();
    const req = httpTesting.expectOne('api/iscsi/target/iqn.foo');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ target_iqn: 'foo' });
  });

  it('should call deleteTarget', () => {
    service.deleteTarget('target_iqn').subscribe();
    const req = httpTesting.expectOne('api/iscsi/target/target_iqn');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call getDiscovery', () => {
    service.getDiscovery().subscribe();
    const req = httpTesting.expectOne('api/iscsi/discoveryauth');
    expect(req.request.method).toBe('GET');
  });

  it('should call updateDiscovery', () => {
    service
      .updateDiscovery({
        user: 'foo',
        password: 'bar',
        mutual_user: 'mutual_foo',
        mutual_password: 'mutual_bar'
      })
      .subscribe();
    const req = httpTesting.expectOne('api/iscsi/discoveryauth');
    expect(req.request.method).toBe('PUT');
  });
});
