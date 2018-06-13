import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { of as observableOf } from 'rxjs';

import { configureTestBed } from '../unit-test-helper';
import { RgwUserService } from './rgw-user.service';

describe('RgwUserService', () => {
  let service: RgwUserService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [RgwUserService]
  });

  beforeEach(() => {
    service = TestBed.get(RgwUserService);
    httpTesting = TestBed.get(HttpTestingController);
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
    const req = httpTesting.expectOne('/api/rgw/proxy/metadata/user');
    expect(req.request.method).toBe('GET');
    req.flush([]);
    expect(result).toEqual([]);
  });

  it('should call list with result', () => {
    let result;
    service.list().subscribe((resp) => {
      result = resp;
    });

    let req = httpTesting.expectOne('/api/rgw/proxy/metadata/user');
    expect(req.request.method).toBe('GET');
    req.flush(['foo', 'bar']);

    req = httpTesting.expectOne('/api/rgw/proxy/user?uid=foo');
    expect(req.request.method).toBe('GET');
    req.flush({ name: 'foo' });

    req = httpTesting.expectOne('/api/rgw/proxy/user?uid=bar');
    expect(req.request.method).toBe('GET');
    req.flush({ name: 'bar' });

    expect(result).toEqual([{ name: 'foo' }, { name: 'bar' }]);
  });

  it('should call enumerate', () => {
    service.enumerate().subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/metadata/user');
    expect(req.request.method).toBe('GET');
  });

  it('should call get', () => {
    service.get('foo').subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/user?uid=foo');
    expect(req.request.method).toBe('GET');
  });

  it('should call getQuota', () => {
    service.getQuota('foo').subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/user?quota&uid=foo');
    expect(req.request.method).toBe('GET');
  });

  it('should call put', () => {
    service.put({ foo: 'bar' }).subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/user?foo=bar');
    expect(req.request.method).toBe('PUT');
  });

  it('should call putQuota', () => {
    service.putQuota({ foo: 'bar' }).subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/user?quota&foo=bar');
    expect(req.request.method).toBe('PUT');
  });

  it('should call post', () => {
    service.post({ foo: 'bar' }).subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/user?foo=bar');
    expect(req.request.method).toBe('POST');
  });

  it('should call delete', () => {
    service.delete('foo').subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/user?uid=foo');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call addSubuser with unrecognized permission', () => {
    service.addSubuser('foo', 'bar', 'baz', null, true).subscribe();
    const req = httpTesting.expectOne(
      '/api/rgw/proxy/user?uid=foo&subuser=bar&key-type=swift&access=baz&generate-secret=true'
    );
    expect(req.request.method).toBe('PUT');
  });

  it('should call addSubuser with mapped permission', () => {
    service.addSubuser('foo', 'bar', 'full-control', 'baz', false).subscribe();
    const req = httpTesting.expectOne(
      '/api/rgw/proxy/user?uid=foo&subuser=bar&key-type=swift&access=full&secret-key=baz'
    );
    expect(req.request.method).toBe('PUT');
  });

  it('should call deleteSubuser', () => {
    service.deleteSubuser('foo', 'bar').subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/user?uid=foo&subuser=bar&purge-keys=true');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call addCapability', () => {
    service.addCapability('foo', 'bar', 'baz').subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/user?caps&uid=foo&user-caps=bar=baz');
    expect(req.request.method).toBe('PUT');
  });

  it('should call deleteCapability', () => {
    service.deleteCapability('foo', 'bar', 'baz').subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/user?caps&uid=foo&user-caps=bar=baz');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call addS3Key, with generateKey true', () => {
    service.addS3Key('1', 'foo', 'bar', 'baz', true).subscribe();
    const req = httpTesting.expectOne(
      '/api/rgw/proxy/user?key&uid=1&key-type=s3&generate-key=true&subuser=foo'
    );
    expect(req.request.method).toBe('PUT');
  });

  it('should call addS3Key, with generateKey false', () => {
    service.addS3Key('1', 'foo', 'bar', 'baz', false).subscribe();
    const req = httpTesting.expectOne(
      '/api/rgw/proxy/user?key' +
        '&uid=1&key-type=s3&generate-key=false&access-key=bar&secret-key=baz&subuser=foo'
    );
    expect(req.request.method).toBe('PUT');
  });

  it('should call deleteS3Key', () => {
    service.deleteS3Key('foo', 'bar').subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/user?key&uid=foo&key-type=s3&access-key=bar');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call exists with an existent uid', () => {
    spyOn(service, 'enumerate').and.returnValue(observableOf(['foo', 'bar']));
    let result;
    service.exists('foo').subscribe((res) => {
      result = res;
    });
    expect(result).toBe(true);
  });

  it('should call exists with a non existent uid', () => {
    spyOn(service, 'enumerate').and.returnValue(observableOf(['foo', 'bar']));
    let result;
    service.exists('baz').subscribe((res) => {
      result = res;
    });
    expect(result).toBe(false);
  });
});
