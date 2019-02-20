import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import { PrometheusService } from '../api/prometheus.service';
import { PrometheusAlert, PrometheusCustomAlert } from '../models/prometheus-alerts';
import { PrometheusAlertFormatter } from './prometheus-alert-formatter';
import { ServicesModule } from './services.module';

@Injectable({
  providedIn: ServicesModule
})
export class PrometheusAlertService {
  private canAlertsBeNotified = false;
  alerts: PrometheusAlert[] = [];

  constructor(
    private alertFormatter: PrometheusAlertFormatter,
    private prometheusService: PrometheusService
  ) {}

  refresh() {
    this.prometheusService.ifAlertmanagerConfigured(() => {
      this.prometheusService.list().subscribe(
        (alerts) => this.handleAlerts(alerts),
        (resp) => {
          if (resp.status === 404 || resp.status === 500) {
            this.prometheusService.disableAlertmanagerConfig();
          }
        }
      );
    });
  }

  private handleAlerts(alerts: PrometheusAlert[]) {
    if (this.canAlertsBeNotified) {
      this.notifyOnAlertChanges(alerts, this.alerts);
    }
    this.alerts = alerts;
    this.canAlertsBeNotified = true;
  }

  private notifyOnAlertChanges(alerts: PrometheusAlert[], oldAlerts: PrometheusAlert[]) {
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
