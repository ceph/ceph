import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { MgrModuleService } from './mgr-module.service';

describe('MgrModuleService', () => {
  let service: MgrModuleService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    imports: [HttpClientTestingModule],
    providers: [MgrModuleService]
  });

  beforeEach(() => {
    service = TestBed.inject(MgrModuleService);
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
    const req = httpTesting.expectOne('api/mgr/module');
    expect(req.request.method).toBe('GET');
  });

  it('should call getConfig', () => {
    service.getConfig('foo').subscribe();
    const req = httpTesting.expectOne('api/mgr/module/foo');
    expect(req.request.method).toBe('GET');
  });

  it('should call updateConfig', () => {
    const config = { foo: 'bar' };
    service.updateConfig('xyz', config).subscribe();
    const req = httpTesting.expectOne('api/mgr/module/xyz');
    expect(req.request.method).toBe('PUT');
    expect(req.request.body.config).toEqual(config);
  });

  it('should call enable', () => {
    service.enable('foo').subscribe();
    const req = httpTesting.expectOne('api/mgr/module/foo/enable');
    expect(req.request.method).toBe('POST');
  });

  it('should call disable', () => {
    service.disable('bar').subscribe();
    const req = httpTesting.expectOne('api/mgr/module/bar/disable');
    expect(req.request.method).toBe('POST');
  });

  it('should call getOptions', () => {
    service.getOptions('foo').subscribe();
    const req = httpTesting.expectOne('api/mgr/module/foo/options');
    expect(req.request.method).toBe('GET');
  });
});
