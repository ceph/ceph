import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { PrometheusService } from '../api/prometheus.service';
import { CdNotificationConfig } from '../models/cd-notification';
import { AlertmanagerNotification } from '../models/prometheus-alerts';
import { PrometheusAlertFormatter } from './prometheus-alert-formatter';

@Injectable({
  providedIn: 'root'
})
export class PrometheusNotificationService {
  private notifications: AlertmanagerNotification[];
  private backendFailure = false;

  constructor(
    private alertFormatter: PrometheusAlertFormatter,
    private prometheusService: PrometheusService
  ) {
    this.notifications = [];
  }

  refresh() {
    if (this.backendFailure) {
      return;
    }
    this.prometheusService.getNotifications(_.last(this.notifications)).subscribe(
      (notifications) => this.handleNotifications(notifications),
      () => (this.backendFailure = true)
    );
  }

  private handleNotifications(notifications: AlertmanagerNotification[]) {
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

  private formatNotification(notification: AlertmanagerNotification): CdNotificationConfig[] {
    return this.alertFormatter
      .convertToCustomAlerts(notification.alerts)
      .map((alert) => this.alertFormatter.convertAlertToNotification(alert));
  }
}
