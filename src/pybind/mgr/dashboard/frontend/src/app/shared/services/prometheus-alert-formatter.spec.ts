import { TestBed } from '@angular/core/testing';

import { ToastModule } from 'ng2-toastr';

import {
  configureTestBed,
  i18nProviders,
  PrometheusHelper
} from '../../../testing/unit-test-helper';
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
    imports: [ToastModule.forRoot(), SharedModule],
    providers: [PrometheusAlertFormatter, i18nProviders]
  });

  beforeEach(() => {
    prometheus = new PrometheusHelper();
    service = TestBed.get(PrometheusAlertFormatter);
    notificationService = TestBed.get(NotificationService);
    spyOn(notificationService, 'queueNotifications').and.stub();
  });

  it('should create', () => {
    expect(service).toBeTruthy();
  });

  describe('sendNotifications', () => {
    it('should not call queue notifications with no notification', () => {
      service.sendNotifications([]);
      expect(notificationService.queueNotifications).not.toHaveBeenCalled();
    });

    it('should call queue notifications with notifications', () => {
      const notifications = [new CdNotificationConfig(NotificationType.success, 'test')];
      service.sendNotifications(notifications);
      expect(notificationService.queueNotifications).toHaveBeenCalledWith(notifications);
    });
  });

  describe('convertToCustomAlert', () => {
    it('converts PrometheusAlert', () => {
      expect(service.convertToCustomAlerts([prometheus.createAlert('Something')])).toEqual([
        {
          status: 'active',
          name: 'Something',
          summary: 'Something is active',
          url: 'http://Something',
          fingerprint: 'Something'
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
          summary: 'Something is firing',
          url: 'http://Something'
        } as PrometheusCustomAlert
      ]);
    });
  });

  it('converts custom alert into notification', () => {
    const alert: PrometheusCustomAlert = {
      status: 'active',
      name: 'Some alert',
      summary: 'Some alert is active',
      url: 'http://some-alert',
      fingerprint: '42'
    };
    expect(service.convertAlertToNotification(alert)).toEqual(
      new CdNotificationConfig(
        NotificationType.error,
        'Some alert (active)',
        'Some alert is active <a href="http://some-alert" target="_blank">' +
          '<i class="fa fa-line-chart"></i></a>',
        undefined,
        'Prometheus'
      )
    );
  });
});
