import { Injectable } from '@angular/core';

import _ from 'lodash';

import { Icons } from '../enum/icons.enum';
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
          description: alert.annotations.description,
          fingerprint: _.isObject(alert.status) && (alert as AlertmanagerAlert).fingerprint,
          labels: alert.labels
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
    const config = new CdNotificationConfig(
      this.formatType(alert?.status, alert?.labels?.severity),
      `${alert.name} (${alert.status})`,
      this.appendSourceLink(alert, alert.description),
      undefined,
      'Prometheus'
    );

    // Add Prometheus-specific metadata
    config['prometheusAlert'] = {
      alertName: alert.name,
      status: alert.status,
      severity: alert.labels?.severity || this.mapStatusToSeverity(alert.status),
      instance: alert.labels?.instance,
      job: alert.labels?.job,
      description: alert.description,
      sourceUrl: alert.url,
      fingerprint: alert.fingerprint ? String(alert.fingerprint) : undefined
    };

    return config;
  }

  private formatType(status: string, severity: string): NotificationType {
    if (severity) {
      switch (severity.toLowerCase()) {
        case 'critical':
          return NotificationType.error;
        case 'warning':
          return NotificationType.warning;
        case 'info':
          return NotificationType.info;
        case 'resolved':
          return NotificationType.success;
      }
    }

    // Fallback: map status if severity not present
    const types = {
      error: ['firing', 'active'],
      info: ['suppressed', 'unprocessed'],
      success: ['resolved']
    };

    return NotificationType[_.findKey(types, (type: any) => type.includes(status))];
  }

  private appendSourceLink(alert: PrometheusCustomAlert, message: string): string {
    return `${message} <a href="${alert.url}" target="_blank"><svg cdsIcon="${Icons.lineChart}" size="${Icons.size16}" ></svg></a>`;
  }

  private mapStatusToSeverity(status: string): string {
    switch (status) {
      case 'active':
      case 'firing':
        return 'critical';
      case 'resolved':
        return 'resolved';
      case 'suppressed':
        return 'suppressed';
      case 'unprocessed':
        return 'warning';
      default:
        return 'unknown';
    }
  }
}
