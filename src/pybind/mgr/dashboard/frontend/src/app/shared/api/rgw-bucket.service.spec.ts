import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
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
    const req = httpTesting.expectOne('api/rgw/bucket');
    req.flush([]);
    expect(req.request.method).toBe('GET');
    expect(result).toEqual([]);
  });

  it('should call list, with enumerate returning 2 elements', () => {
    let result;
    service.list().subscribe((resp) => {
      result = resp;
    });
    let req = httpTesting.expectOne('api/rgw/bucket');
    req.flush(['foo', 'bar']);

    req = httpTesting.expectOne('api/rgw/bucket/foo');
    req.flush({ name: 'foo' });

    req = httpTesting.expectOne('api/rgw/bucket/bar');
    req.flush({ name: 'bar' });

    expect(req.request.method).toBe('GET');
    expect(result).toEqual([{ name: 'foo' }, { name: 'bar' }]);
  });

  it('should call get', () => {
    service.get('foo').subscribe();
    const req = httpTesting.expectOne('api/rgw/bucket/foo');
    expect(req.request.method).toBe('GET');
  });

  it('should call create', () => {
    service.create('foo', 'bar', 'default', 'default-placement').subscribe();
    const req = httpTesting.expectOne(
      'api/rgw/bucket?bucket=foo&uid=bar&zonegroup=default&placement_target=default-placement'
    );
    expect(req.request.method).toBe('POST');
  });

  it('should call update', () => {
    service.update('foo', 'bar', 'baz', 'Enabled').subscribe();
    const req = httpTesting.expectOne(
      'api/rgw/bucket/foo?bucket_id=bar&uid=baz&versioning_state=Enabled'
    );
    expect(req.request.method).toBe('PUT');
  });

  it('should call delete, with purgeObjects = true', () => {
    service.delete('foo').subscribe();
    const req = httpTesting.expectOne('api/rgw/bucket/foo?purge_objects=true');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call delete, with purgeObjects = false', () => {
    service.delete('foo', false).subscribe();
    const req = httpTesting.expectOne('api/rgw/bucket/foo?purge_objects=false');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call exists', () => {
    let result;
    service.exists('foo').subscribe((resp) => {
      result = resp;
    });
    const req = httpTesting.expectOne('api/rgw/bucket');
    expect(req.request.method).toBe('GET');
    req.flush(['foo', 'bar']);
    expect(result).toBe(true);
  });
});
