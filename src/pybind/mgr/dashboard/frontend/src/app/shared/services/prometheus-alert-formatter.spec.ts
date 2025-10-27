import { HttpClientTestingModule } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';

import { ToastrModule } from 'ngx-toastr';

import { configureTestBed, PrometheusHelper } from '~/testing/unit-test-helper';
import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import { PrometheusCustomAlert } from '../models/prometheus-alerts';
import { SharedModule } from '../shared.module';
import { NotificationService } from './notification.service';
import { PrometheusAlertFormatter } from './prometheus-alert-formatter';

describe('PrometheusAlertFormatter', () => {
  let service: PrometheusAlertFormatter;
  let notificationService: NotificationService;
  let prometheus: PrometheusHelper;

  configureTestBed({
    imports: [ToastrModule.forRoot(), SharedModule, HttpClientTestingModule],
    providers: [PrometheusAlertFormatter]
  });

  beforeEach(() => {
    prometheus = new PrometheusHelper();
    service = TestBed.inject(PrometheusAlertFormatter);
    notificationService = TestBed.inject(NotificationService);
    spyOn(notificationService, 'show').and.stub();
  });

  it('should create', () => {
    expect(service).toBeTruthy();
  });

  describe('sendNotifications', () => {
    it('should not call queue notifications with no notification', () => {
      service.sendNotifications([]);
      expect(notificationService.show).not.toHaveBeenCalled();
    });

    it('should call queue notifications with notifications', () => {
      const notifications = [new CdNotificationConfig(NotificationType.success, 'test')];
      service.sendNotifications(notifications);
      expect(notificationService.show).toHaveBeenCalledWith(notifications[0]);
    });
  });

  describe('convertToCustomAlert', () => {
    it('converts PrometheusAlert', () => {
      expect(service.convertToCustomAlerts([prometheus.createAlert('Something')])).toEqual([
        {
          status: 'active',
          name: 'Something',
          description: 'Something is active',
          url: 'http://Something',
          fingerprint: 'Something',
          severity: 'someSeverity'
        } as PrometheusCustomAlert
      ]);
    });

    it('converts PrometheusNotificationAlert', () => {
      expect(
        service.convertToCustomAlerts([prometheus.createNotificationAlert('Something')])
      ).toEqual([
        {
          fingerprint: false,
          status: 'active',
          name: 'Something',
          description: 'Something is firing',
          url: 'http://Something',
          severity: undefined
        } as PrometheusCustomAlert
      ]);
    });
  });

  it('converts custom alert into notification', () => {
    const alert: PrometheusCustomAlert = {
      status: 'active',
      name: 'Some alert',
      description: 'Some alert is active',
      url: 'http://some-alert',
      fingerprint: '42',
      severity: 'critical'
    };
    expect(service.convertAlertToNotification(alert)).toEqual(
      new CdNotificationConfig(
        NotificationType.error,
        'Some alert (active)',
        'Some alert is active <a href="http://some-alert" target="_blank">' +
          '<svg cdsIcon="analytics" size="16" ></svg></a>',
        undefined,
        'Prometheus'
      )
    );
  });

  it('converts warning alert into warning notification', () => {
    const alert: PrometheusCustomAlert = {
      status: 'active',
      name: 'Warning alert',
      description: 'Warning alert is active',
      url: 'http://warning-alert',
      fingerprint: '43',
      severity: 'warning'
    };
    expect(service.convertAlertToNotification(alert)).toEqual(
      new CdNotificationConfig(
        NotificationType.warning,
        'Warning alert (active)',
        'Warning alert is active <a href="http://warning-alert" target="_blank">' +
          '<svg cdsIcon="analytics" size="16" ></svg></a>',
        undefined,
        'Prometheus'
      )
    );
  });
});
