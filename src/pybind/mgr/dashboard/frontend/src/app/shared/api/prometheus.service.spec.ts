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

  it('should call listSilences', () => {
    service.listSilences().subscribe();
    const req = httpTesting.expectOne('api/prometheus/silences');
    expect(req.request.method).toBe('GET');
  });

  it('should call setSilence', () => {
    const silence = {
      id: 'someId',
      matchers: [
        {
          name: 'getZero',
          value: 0,
          isRegex: false
        }
      ],
      startsAt: '2019-01-25T14:32:46.646300974Z',
      endsAt: '2019-01-25T18:32:46.646300974Z',
      createdBy: 'someCreator',
      comment: 'for testing purpose'
    };
    service.setSilence(silence).subscribe();
    const req = httpTesting.expectOne('api/prometheus/silence');
    expect(req.request.method).toBe('POST');
    expect(req.request.body).toEqual(silence);
  });

  it('should call expireSilence', () => {
    service.expireSilence('someId').subscribe();
    const req = httpTesting.expectOne('api/prometheus/silence/someId');
    expect(req.request.method).toBe('DELETE');
  });

  it('should call getNotificationSince', () => {
    service.getNotificationSince({}).subscribe();
    const req = httpTesting.expectOne('api/prometheus/get_notifications_since');
    expect(req.request.method).toBe('POST');
  });

  describe('ifAlertmanagerConfigured', () => {
    let x: any;
    let host;

    const receiveConfig = () => {
      const req = httpTesting.expectOne('api/settings/alertmanager-api-host');
      expect(req.request.method).toBe('GET');
      req.flush({ value: host });
    };

    beforeEach(() => {
      x = false;
      TestBed.get(SettingsService)['settings'] = {};
      service.ifAlertmanagerConfigured((v) => (x = v));
      host = 'http://localhost:9093';
    });

    it('changes x in a valid case', () => {
      expect(x).toBe(false);
      receiveConfig();
      expect(x).toBe(host);
    });

    it('does not change x in a invalid case', () => {
      host = '';
      receiveConfig();
      expect(x).toBe(false);
    });

    it('disables the set setting', () => {
      receiveConfig();
      service.disableAlertmanagerConfig();
      x = false;
      service.ifAlertmanagerConfigured((v) => (x = v));
      expect(x).toBe(false);
    });
  });

  describe('ifPrometheusConfigured', () => {
    let x: any;
    let host;

    const receiveConfig = () => {
      const req = httpTesting.expectOne('api/settings/prometheus-api-host');
      expect(req.request.method).toBe('GET');
      req.flush({ value: host });
    };

    beforeEach(() => {
      x = false;
      TestBed.get(SettingsService)['settings'] = {};
      service.ifPrometheusConfigured((v) => (x = v));
      host = 'http://localhost:9090';
    });

    it('changes x in a valid case', () => {
      expect(x).toBe(false);
      receiveConfig();
      expect(x).toBe(host);
    });

    it('does not change x in a invalid case', () => {
      host = '';
      receiveConfig();
      expect(x).toBe(false);
    });

    it('disables the set setting', () => {
      receiveConfig();
      service.disablePrometheusConfig();
      x = false;
      service.ifPrometheusConfigured((v) => (x = v));
      expect(x).toBe(false);
    });
  });
});
