import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { of as observableOf, throwError } from 'rxjs';

import { configureTestBed, RgwHelper } from '~/testing/unit-test-helper';
import { RgwUserService } from './rgw-user.service';

describe('RgwUserService', () => {
  let service: RgwUserService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [RgwUserService]
  });

  beforeEach(() => {
    service = TestBed.inject(RgwUserService);
    httpTesting = TestBed.inject(HttpTestingController);
    RgwHelper.selectDaemon();
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list with empty result', () => {
    let result;
    service.list().subscribe((resp) => {
      result = resp;
    });
    const req = httpTesting.expectOne(
      `api/rgw/user?${RgwHelper.DAEMON_QUERY_PARAM}&detailed=false`
    );
    expect(req.request.method).toBe('GET');
    req.flush([]);
    expect(result).toEqual([]);
  });

  it('should call list with result', () => {
    let result;
    service.list().subscribe((resp) => {
      result = resp;
    });
    let req = httpTesting.expectOne(`api/rgw/user?${RgwHelper.DAEMON_QUERY_PARAM}&detailed=false`);
    expect(req.request.method).toBe('GET');
    req.flush(['foo', 'bar']);

    req = httpTesting.expectOne(`api/rgw/user/foo?${RgwHelper.DAEMON_QUERY_PARAM}`);
    expect(req.request.method).toBe('GET');
    req.flush({ name: 'foo' });

    req = httpTesting.expectOne(`api/rgw/user/bar?${RgwHelper.DAEMON_QUERY_PARAM}`);
    expect(req.request.method).toBe('GET');
    req.flush({ name: 'bar' });

    expect(result).toEqual([{ name: 'foo' }, { name: 'bar' }]);
  });

  it('should call enumerate', () => {
    service.enumerate().subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/user?${RgwHelper.DAEMON_QUERY_PARAM}&detailed=false`
    );
    expect(req.request.method).toBe('GET');
  });

  it('should call get', () => {
    service.get('foo').subscribe();
    const req = httpTesting.expectOne(`api/rgw/user/foo?${RgwHelper.DAEMON_QUERY_PARAM}`);
    expect(req.request.method).toBe('GET');
  });

  it('should call getQuota', () => {
    service.getQuota('foo').subscribe();
    const req = httpTesting.expectOne(`api/rgw/user/foo/quota?${RgwHelper.DAEMON_QUERY_PARAM}`);
    expect(req.request.method).toBe('GET');
  });

  it('should call update', () => {
    service.update('foo', { xxx: 'yyy' }).subscribe();
    const req = httpTesting.expectOne(`api/rgw/user/foo?${RgwHelper.DAEMON_QUERY_PARAM}&xxx=yyy`);
    expect(req.request.method).toBe('PUT');
  });

  it('should call updateQuota', () => {
    service.updateQuota('foo', { xxx: 'yyy' }).subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/user/foo/quota?${RgwHelper.DAEMON_QUERY_PARAM}&xxx=yyy`
    );
    expect(req.request.method).toBe('PUT');
  });

  it('should call create', () => {
    service.create({ foo: 'bar' }).subscribe();
    const req = httpTesting.expectOne(`api/rgw/user?${RgwHelper.DAEMON_QUERY_PARAM}&foo=bar`);
    expect(req.request.method).toBe('POST');
  });

  it('should call delete', () => {
    service.delete('foo').subscribe();
    const req = httpTesting.expectOne(`api/rgw/user/foo?${RgwHelper.DAEMON_QUERY_PARAM}`);
    expect(req.request.method).toBe('DELETE');
  });

  it('should call createSubuser', () => {
    service.createSubuser('foo', { xxx: 'yyy' }).subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/user/foo/subuser?${RgwHelper.DAEMON_QUERY_PARAM}&xxx=yyy`
    );
    expect(req.request.method).toBe('POST');
  });

  it('should call deleteSubuser', () => {
    service.deleteSubuser('foo', 'bar').subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/user/foo/subuser/bar?${RgwHelper.DAEMON_QUERY_PARAM}`
    );
    expect(req.request.method).toBe('DELETE');
  });

  it('should call addCapability', () => {
    service.addCapability('foo', 'bar', 'baz').subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/user/foo/capability?${RgwHelper.DAEMON_QUERY_PARAM}&type=bar&perm=baz`
    );
    expect(req.request.method).toBe('POST');
  });

  it('should call deleteCapability', () => {
    service.deleteCapability('foo', 'bar', 'baz').subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/user/foo/capability?${RgwHelper.DAEMON_QUERY_PARAM}&type=bar&perm=baz`
    );
    expect(req.request.method).toBe('DELETE');
  });

  it('should call addS3Key', () => {
    service.addS3Key('foo', { xxx: 'yyy' }).subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/user/foo/key?${RgwHelper.DAEMON_QUERY_PARAM}&key_type=s3&xxx=yyy`
    );
    expect(req.request.method).toBe('POST');
  });

  it('should call deleteS3Key', () => {
    service.deleteS3Key('foo', 'bar').subscribe();
    const req = httpTesting.expectOne(
      `api/rgw/user/foo/key?${RgwHelper.DAEMON_QUERY_PARAM}&key_type=s3&access_key=bar`
    );
    expect(req.request.method).toBe('DELETE');
  });

  it('should call exists with an existent uid', (done) => {
    spyOn(service, 'get').and.returnValue(observableOf({}));
    service.exists('foo').subscribe((res) => {
      expect(res).toBe(true);
      done();
    });
  });

  it('should call exists with a non existent uid', (done) => {
    spyOn(service, 'get').and.returnValue(throwError('bar'));
    service.exists('baz').subscribe((res) => {
      expect(res).toBe(false);
      done();
    });
  });
});
