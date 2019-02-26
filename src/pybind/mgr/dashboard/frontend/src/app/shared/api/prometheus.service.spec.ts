import { HttpClientTestingModule, HttpTestingController } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { configureTestBed } from '../../../testing/unit-test-helper';
import { PrometheusService } from './prometheus.service';
import { SettingsService } from './settings.service';

describe('PrometheusService', () => {
  let service: PrometheusService;
  let httpTesting: HttpTestingController;

  configureTestBed({
    providers: [PrometheusService, SettingsService],
    imports: [HttpClientTestingModule]
  });

  beforeEach(() => {
    service = TestBed.get(PrometheusService);
    httpTesting = TestBed.get(HttpTestingController);
  });

  afterEach(() => {
    httpTesting.verify();
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('should call list', () => {
    service.list().subscribe();
    const req = httpTesting.expectOne('api/prometheus');
    expect(req.request.method).toBe('GET');
  });

  it('should call getNotificationSince', () => {
    service.getNotificationSince({}).subscribe();
    const req = httpTesting.expectOne('api/prometheus/get_notifications_since');
    expect(req.request.method).toBe('POST');
  });

  describe('ifAlertmanagerConfigured', () => {
    let x: any;

    const receiveConfig = (value) => {
      const req = httpTesting.expectOne('api/settings/alertmanager-api-host');
      expect(req.request.method).toBe('GET');
      req.flush({ value });
    };

    beforeEach(() => {
      x = false;
      TestBed.get(SettingsService)['settings'] = {};
    });

    it('changes x in a valid case', () => {
      service.ifAlertmanagerConfigured((v) => (x = v));
      expect(x).toBe(false);
      const host = 'http://localhost:9093';
      receiveConfig(host);
      expect(x).toBe(host);
    });

    it('does not change x in a invalid case', () => {
      service.ifAlertmanagerConfigured((v) => (x = v));
      receiveConfig('');
      expect(x).toBe(false);
    });
  });
});
