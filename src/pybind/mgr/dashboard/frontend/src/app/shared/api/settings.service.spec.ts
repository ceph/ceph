import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { SettingsService } from './settings.service';

describe('SettingsService', () => {
  let service: SettingsService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [SettingsService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(SettingsService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should get protocol', () => {
    service.getGrafanaApiUrl().subscribe();
    const req = httpTesting.expectOne('api/settings/GRAFANA_API_URL');
    expect(req.request.method).toBe('GET');
  });
});
