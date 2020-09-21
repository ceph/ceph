import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';
import { Observable, of } from 'rxjs';

import { configureTestBed, PrometheusHelper } from '../../../testing/unit-test-helper';
import { PrometheusService } from '../api/prometheus.service';
import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import { AlertmanagerAlert } from '../models/prometheus-alerts';
import { SharedModule } from '../shared.module';
import { NotificationService } from './notification.service';
import { PrometheusAlertFormatter } from './prometheus-alert-formatter';
import { PrometheusAlertService } from './prometheus-alert.service';

describe('PrometheusAlertService', () => {
  let service: PrometheusAlertService;
  let notificationService: NotificationService;
  let alerts: AlertmanagerAlert[];
  let prometheusService: PrometheusService;
  let prometheus: PrometheusHelper;

  configureTestBed({
    imports: [ToastrModule.forRoot(), SharedModule, HttpClientTestingModule],
    providers: [PrometheusAlertService, PrometheusAlertFormatter]
  });

  beforeEach(() => {
    prometheus = new PrometheusHelper();
  });

  it('should create', () => {
    expect(TestBed.inject(PrometheusAlertService)).toBeTruthy();
  });

  describe('test failing status codes and verify disabling of the alertmanager', () => {
    const isDisabledByStatusCode = (statusCode: number, expectedStatus: boolean, done: any) => {
      service = TestBed.inject(PrometheusAlertService);
      prometheusService = TestBed.inject(PrometheusService);
      spyOn(prometheusService, 'ifAlertmanagerConfigured').and.callFake((fn) => fn());
      spyOn(prometheusService, 'getAlerts').and.returnValue(
        new Observable((observer: any) => observer.error({ status: statusCode, error: {} }))
      );
      const disableFn = spyOn(prometheusService, 'disableAlertmanagerConfig').and.callFake(() => {
        expect(expectedStatus).toBe(true);
        done();
      });

      if (!expectedStatus) {
        expect(disableFn).not.toHaveBeenCalled();
        done();
      }

      service.getAlerts();
    };

    it('disables on 504 error which is thrown if the mgr failed', (done) => {
      isDisabledByStatusCode(504, true, done);
    });

    it('disables on 404 error which is thrown if the external api cannot be reached', (done) => {
      isDisabledByStatusCode(404, true, done);
    });

    it('does not disable on 400 error which is thrown if the external api receives unexpected data', (done) => {
      isDisabledByStatusCode(400, false, done);
    });
  });

  it('should flatten the response of getRules()', () => {
    service = TestBed.inject(PrometheusAlertService);
    prometheusService = TestBed.inject(PrometheusService);

    spyOn(service['prometheusService'], 'ifPrometheusConfigured').and.callFake((fn) => fn());
    spyOn(prometheusService, 'getRules').and.returnValue(
      of({
        groups: [
          {
            name: 'group1',
            rules: [{ name: 'nearly_full', type: 'alerting' }]
          },
          {
            name: 'test',
            rules: [
              { name: 'load_0', type: 'alerting' },
              { name: 'load_1', type: 'alerting' },
              { name: 'load_2', type: 'alerting' }
            ]
          }
        ]
      })
    );

    service.getRules();

    expect(service.rules as any).toEqual([
      { name: 'nearly_full', type: 'alerting', group: 'group1' },
      { name: 'load_0', type: 'alerting', group: 'test' },
      { name: 'load_1', type: 'alerting', group: 'test' },
      { name: 'load_2', type: 'alerting', group: 'test' }
    ]);
  });

  describe('refresh', () => {
    beforeEach(() => {
      service = TestBed.inject(PrometheusAlertService);
      service['alerts'] = [];
      service['canAlertsBeNotified'] = false;

      spyOn(window, 'setTimeout').and.callFake((fn: Function) => fn());

      notificationService = TestBed.inject(NotificationService);
      spyOn(notificationService, 'show').and.stub();

      prometheusService = TestBed.inject(PrometheusService);
      spyOn(prometheusService, 'ifAlertmanagerConfigured').and.callFake((fn) => fn());
      spyOn(prometheusService, 'getAlerts').and.callFake(() => of(alerts));

      alerts = [prometheus.createAlert('alert0')];
      service.refresh();
    });

    it('should not notify on first call', () => {
      expect(notificationService.show).not.toHaveBeenCalled();
    });

    it('should not notify with no change', () => {
      service.refresh();
      expect(notificationService.show).not.toHaveBeenCalled();
    });

    it('should notify on alert change', () => {
      alerts = [prometheus.createAlert('alert0', 'suppressed')];
      service.refresh();
      expect(notificationService.show).toHaveBeenCalledWith(
        new CdNotificationConfig(
          NotificationType.info,
          'alert0 (suppressed)',
          'alert0 is suppressed ' + prometheus.createLink('http://alert0'),
          undefined,
          'Prometheus'
        )
      );
    });

    it('should notify on a new alert', () => {
      alerts = [prometheus.createAlert('alert1'), prometheus.createAlert('alert0')];
      service.refresh();
      expect(notificationService.show).toHaveBeenCalledTimes(1);
      expect(notificationService.show).toHaveBeenCalledWith(
        new CdNotificationConfig(
          NotificationType.error,
          'alert1 (active)',
          'alert1 is active ' + prometheus.createLink('http://alert1'),
          undefined,
          'Prometheus'
        )
      );
    });

    it('should notify a resolved alert if it is not there anymore', () => {
      alerts = [];
      service.refresh();
      expect(notificationService.show).toHaveBeenCalledTimes(1);
      expect(notificationService.show).toHaveBeenCalledWith(
        new CdNotificationConfig(
          NotificationType.success,
          'alert0 (resolved)',
          'alert0 is active ' + prometheus.createLink('http://alert0'),
          undefined,
          'Prometheus'
        )
      );
    });

    it('should call multiple times for multiple changes', () => {
      const alert1 = prometheus.createAlert('alert1');
      alerts.push(alert1);
      service.refresh();
      alerts = [alert1, prometheus.createAlert('alert2')];
      service.refresh();
      expect(notificationService.show).toHaveBeenCalledTimes(2);
    });
  });
});
