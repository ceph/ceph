import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { Icons } from '../../shared/enum/icons.enum';
import { NotificationType } from '../enum/notification-type.enum';
import { CdNotificationConfig } from '../models/cd-notification';
import {
  AlertmanagerAlert,
  AlertmanagerNotificationAlert,
  PrometheusCustomAlert
} from '../models/prometheus-alerts';
import { NotificationService } from './notification.service';

@Injectable({
  providedIn: 'root'
})
export class PrometheusAlertFormatter {
  constructor(private notificationService: NotificationService) {}

  sendNotifications(notifications: CdNotificationConfig[]) {
    notifications.forEach((n) => this.notificationService.show(n));
  }

  convertToCustomAlerts(
    alerts: (AlertmanagerNotificationAlert | AlertmanagerAlert)[]
  ): PrometheusCustomAlert[] {
    return _.uniqWith(
      alerts.map((alert) => {
        return {
          status: _.isObject(alert.status)
            ? (alert as AlertmanagerAlert).status.state
            : this.getPrometheusNotificationStatus(alert as AlertmanagerNotificationAlert),
          name: alert.labels.alertname,
          url: alert.generatorURL,
          summary: alert.annotations.summary,
          fingerprint: _.isObject(alert.status) && (alert as AlertmanagerAlert).fingerprint
        };
      }),
      _.isEqual
    ) as PrometheusCustomAlert[];
  }

  /*
   * This is needed because NotificationAlerts don't use 'active'
   */
  private getPrometheusNotificationStatus(alert: AlertmanagerNotificationAlert): string {
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
    return `${message} <a href="${alert.url}" target="_blank"><i class="${Icons.lineChart}"></i></a>`;
  }
}
