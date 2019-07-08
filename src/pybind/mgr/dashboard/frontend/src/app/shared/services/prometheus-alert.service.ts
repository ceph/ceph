import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { PrometheusService } from '../api/prometheus.service';
import { AlertmanagerAlert, PrometheusCustomAlert } from '../models/prometheus-alerts';
import { PrometheusAlertFormatter } from './prometheus-alert-formatter';

@Injectable({
  providedIn: 'root'
})
export class PrometheusAlertService {
  private canAlertsBeNotified = false;
  alerts: AlertmanagerAlert[] = [];

  constructor(
    private alertFormatter: PrometheusAlertFormatter,
    private prometheusService: PrometheusService
  ) {}

  refresh() {
    this.prometheusService.ifAlertmanagerConfigured(() => {
      this.prometheusService.getAlerts().subscribe(
        (alerts) => this.handleAlerts(alerts),
        (resp) => {
          if ([404, 504].includes(resp.status)) {
            this.prometheusService.disableAlertmanagerConfig();
          }
        }
      );
    });
  }

  private handleAlerts(alerts: AlertmanagerAlert[]) {
    if (this.canAlertsBeNotified) {
      this.notifyOnAlertChanges(alerts, this.alerts);
    }
    this.alerts = alerts;
    this.canAlertsBeNotified = true;
  }

  private notifyOnAlertChanges(alerts: AlertmanagerAlert[], oldAlerts: AlertmanagerAlert[]) {
    const changedAlerts = this.getChangedAlerts(
      this.alertFormatter.convertToCustomAlerts(alerts),
      this.alertFormatter.convertToCustomAlerts(oldAlerts)
    );
    const notifications = changedAlerts.map((alert) =>
      this.alertFormatter.convertAlertToNotification(alert)
    );
    this.alertFormatter.sendNotifications(notifications);
  }

  private getChangedAlerts(alerts: PrometheusCustomAlert[], oldAlerts: PrometheusCustomAlert[]) {
    const updatedAndNew = _.differenceWith(alerts, oldAlerts, _.isEqual);
    return updatedAndNew.concat(this.getVanishedAlerts(alerts, oldAlerts));
  }

  private getVanishedAlerts(alerts: PrometheusCustomAlert[], oldAlerts: PrometheusCustomAlert[]) {
    return _.differenceWith(oldAlerts, alerts, (a, b) => a.fingerprint === b.fingerprint).map(
      (alert) => {
        alert.status = 'resolved';
        return alert;
      }
    );
  }
}
