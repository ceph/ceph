import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../unit-test-helper';
import { RgwBucketService } from './rgw-bucket.service';

describe('RgwBucketService', () => {
  let service: RgwBucketService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [RgwBucketService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(RgwBucketService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list, with enumerate returning empty', () => {
    let result;
    service.list().subscribe((resp) => {
      result = resp;
    });
    const req = httpTesting.expectOne('/api/rgw/proxy/bucket');
    req.flush([]);
    expect(req.request.method).toBe('GET');
    expect(result).toEqual([]);
  });

  it('should call list, with enumerate returning 2 elements', () => {
    let result;
    service.list().subscribe((resp) => {
      result = resp;
    });
    let req = httpTesting.expectOne('/api/rgw/proxy/bucket');
    req.flush(['foo', 'bar']);

    req = httpTesting.expectOne('/api/rgw/proxy/bucket?bucket=foo');
    req.flush({ name: 'foo' });

    req = httpTesting.expectOne('/api/rgw/proxy/bucket?bucket=bar');
    req.flush({ name: 'bar' });

    expect(req.request.method).toBe('GET');
    expect(result).toEqual([{ name: 'foo' }, { name: 'bar' }]);
  });

  it('should call get', () => {
    service.get('foo').subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/bucket?bucket=foo');
    expect(req.request.method).toBe('GET');
  });

  it('should call create', () => {
    service.create('foo', 'bar').subscribe();
    const req = httpTesting.expectOne('/api/rgw/bucket');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({
      bucket: 'foo',
      uid: 'bar'
    });
  });

  it('should call update', () => {
    service.update('foo', 'bar', 'baz').subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/bucket?bucket=bar&bucket-id=foo&uid=baz');
    expect(req.request.method).toBe('PUT');
  });

  it('should call delete, with purgeObjects = true', () => {
    service.delete('foo').subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/bucket?bucket=foo&purge-objects=true');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call delete, with purgeObjects = false', () => {
    service.delete('foo', false).subscribe();
    const req = httpTesting.expectOne('/api/rgw/proxy/bucket?bucket=foo&purge-objects=false');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call exists', () => {
    let result;
    service.exists('foo').subscribe((resp) => {
      result = resp;
    });
    const req = httpTesting.expectOne('/api/rgw/proxy/bucket');
    expect(req.request.method).toBe('GET');
    req.flush(['foo', 'bar']);
    expect(result).toBe(true);
  });
});
