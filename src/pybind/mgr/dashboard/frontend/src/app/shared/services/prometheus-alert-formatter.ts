import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import {
  PrometheusAlert,
  PrometheusCustomAlert,
  PrometheusNotificationAlert
} from '../models/prometheus-alerts';
import { NotificationService } from './notification.service';
import { ServicesModule } from './services.module';

@Injectable({
  providedIn: ServicesModule
})
export class PrometheusAlertFormatter {
  constructor(private notificationService: NotificationService) {}

  sendNotifications(notifications: CdNotificationConfig[]) {
    if (notifications.length > 0) {
      this.notificationService.queueNotifications(notifications);
    }
  }

  convertToCustomAlerts(
    alerts: (PrometheusNotificationAlert | PrometheusAlert)[]
  ): PrometheusCustomAlert[] {
    return _.uniqWith(
      alerts.map((alert) => {
        return {
          status: _.isObject(alert.status)
            ? (alert as PrometheusAlert).status.state
            : this.getPrometheusNotificationStatus(alert as PrometheusNotificationAlert),
          name: alert.labels.alertname,
          url: alert.generatorURL,
          summary: alert.annotations.summary,
          fingerprint: _.isObject(alert.status) && (alert as PrometheusAlert).fingerprint
        };
      }),
      _.isEqual
    ) as PrometheusCustomAlert[];
  }

  /*
   * This is needed because NotificationAlerts don't use 'active'
   */
  private getPrometheusNotificationStatus(alert: PrometheusNotificationAlert): string {
    const state = alert.status;
    return state === 'firing' ? 'active' : state;
  }

  convertAlertToNotification(alert: PrometheusCustomAlert): CdNotificationConfig {
    return new CdNotificationConfig(
      this.formatType(alert.status),
      `${alert.name} (${alert.status})`,
      this.appendSourceLink(alert, alert.summary),
      undefined,
      'Prometheus'
    );
  }

  private formatType(status: string): NotificationType {
    const types = {
      error: ['firing', 'active'],
      info: ['suppressed', 'unprocessed'],
      success: ['resolved']
    };
    return NotificationType[_.findKey(types, (type) => type.includes(status))];
  }

  private appendSourceLink(alert: PrometheusCustomAlert, message: string): string {
    return `${message} <a href="${alert.url}" target="_blank"><i class="fa fa-line-chart"></i></a>`;
  }
}
