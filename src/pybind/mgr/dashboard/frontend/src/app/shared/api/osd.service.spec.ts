import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { OsdService } from './osd.service';

describe('OsdService', () => {
  let service: OsdService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [OsdService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(OsdService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getList', () => {
    service.getList().subscribe();
    const req = httpTesting.expectOne('api/osd');
    expect(req.request.method).toBe('GET');
  });

  it('should call getDetails', () => {
    service.getDetails(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1');
    expect(req.request.method).toBe('GET');
  });

  it('should call scrub, with deep=true', () => {
    service.scrub('foo', true).subscribe();
    const req = httpTesting.expectOne('api/osd/foo/scrub?deep=true');
    expect(req.request.method).toBe('POST');
  });

  it('should call scrub, with deep=false', () => {
    service.scrub('foo', false).subscribe();
    const req = httpTesting.expectOne('api/osd/foo/scrub?deep=false');
    expect(req.request.method).toBe('POST');
  });

  it('should call getFlags', () => {
    service.getFlags().subscribe();
    const req = httpTesting.expectOne('api/osd/flags');
    expect(req.request.method).toBe('GET');
  });

  it('should call updateFlags', () => {
    service.updateFlags(['foo']).subscribe();
    const req = httpTesting.expectOne('api/osd/flags');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body).toEqual({ flags: ['foo'] });
  });
});
