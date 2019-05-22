import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { PrometheusService } from '../api/prometheus.service';
import { CdNotificationConfig } from '../models/cd-notification';
import { PrometheusNotification } from '../models/prometheus-alerts';
import { PrometheusAlertFormatter } from './prometheus-alert-formatter';

@Injectable({
  providedIn: 'root'
})
export class PrometheusNotificationService {
  private notifications: PrometheusNotification[];

  constructor(
    private alertFormatter: PrometheusAlertFormatter,
    private prometheusService: PrometheusService
  ) {
    this.notifications = [];
  }

  refresh() {
    this.prometheusService
      .getNotifications(_.last(this.notifications))
      .subscribe((notifications) => this.handleNotifications(notifications));
  }

  private handleNotifications(notifications: PrometheusNotification[]) {
    if (notifications.length === 0) {
      return;
    }
    if (this.notifications.length > 0) {
      this.alertFormatter.sendNotifications(
        _.flatten(notifications.map((notification) => this.formatNotification(notification)))
      );
    }
    this.notifications = this.notifications.concat(notifications);
  }

  private formatNotification(notification: PrometheusNotification): CdNotificationConfig[] {
    return this.alertFormatter
      .convertToCustomAlerts(notification.alerts)
      .map((alert) => this.alertFormatter.convertAlertToNotification(alert));
  }
}
