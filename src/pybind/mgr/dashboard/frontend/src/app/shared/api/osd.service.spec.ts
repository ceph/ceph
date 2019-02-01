import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed, i18nProviders } from '../../../testing/unit-test-helper';
import { OsdService } from './osd.service';

describe('OsdService', () => {
  let service: OsdService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [OsdService, i18nProviders],
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

  it('should mark the OSD out', () => {
    service.markOut(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/mark_out');
    expect(req.request.method).toBe('POST');
  });

  it('should mark the OSD in', () => {
    service.markIn(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/mark_in');
    expect(req.request.method).toBe('POST');
  });

  it('should mark the OSD down', () => {
    service.markDown(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/mark_down');
    expect(req.request.method).toBe('POST');
  });

  it('should reweight an OSD', () => {
    service.reweight(1, 0.5).subscribe();
    const req = httpTesting.expectOne('api/osd/1/reweight');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual({ weight: 0.5 });
  });

  it('should mark an OSD lost', () => {
    service.markLost(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/mark_lost');
    expect(req.request.method).toBe('POST');
  });

  it('should purge an OSD', () => {
    service.purge(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/purge');
    expect(req.request.method).toBe('POST');
  });

  it('should destroy an OSD', () => {
    service.destroy(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/destroy');
    expect(req.request.method).toBe('POST');
  });

  it('should return if it is safe to destroy an OSD', () => {
    service.safeToDestroy(1).subscribe();
    const req = httpTesting.expectOne('api/osd/1/safe_to_destroy');
    expect(req.request.method).toBe('GET');
  });
});
