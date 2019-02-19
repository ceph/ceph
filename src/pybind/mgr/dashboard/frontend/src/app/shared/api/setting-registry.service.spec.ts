import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { SettingRegistryService } from './setting-registry.service';

describe('SettingRegistryService', () => {
  let service: SettingRegistryService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [SettingRegistryService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(SettingRegistryService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call getSettingsList', () => {
    service.getSettingsList().subscribe();
    const req = httpTesting.expectOne('ui-api/uisetting/get_setting');
    expect(req.request.method).toBe('GET');
  });

  it('should call update', () => {
    service.updateSetting('foo', { xxx: 'yyy' }).subscribe();
    const req = httpTesting.expectOne('ui-api/uisetting/foo');
    expect(req.request.method).toBe('PUT');
  });
});
