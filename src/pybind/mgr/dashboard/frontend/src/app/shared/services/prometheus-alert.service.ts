import { Injectable } from '@angular/core';

import _ from 'lodash';

import { PrometheusService } from '../api/prometheus.service';
import {
  AlertmanagerAlert,
  PrometheusCustomAlert,
  PrometheusRule
} from '../models/prometheus-alerts';
import { PrometheusAlertFormatter } from './prometheus-alert-formatter';

@Injectable({
  providedIn: 'root'
})
export class PrometheusAlertService {
  private canAlertsBeNotified = false;
  alerts: AlertmanagerAlert[] = [];
  rules: PrometheusRule[] = [];
  activeAlerts: number;
  activeCriticalAlerts: number;
  activeWarningAlerts: number;

  constructor(
    private alertFormatter: PrometheusAlertFormatter,
    private prometheusService: PrometheusService
  ) {}

  getAlerts() {
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

  getRules() {
    this.prometheusService.ifPrometheusConfigured(() => {
      this.prometheusService.getRules('alerting').subscribe((groups) => {
        this.rules = groups['groups'].reduce((acc, group) => {
          return acc.concat(
            group.rules.map((rule) => {
              rule.group = group.name;
              return rule;
            })
          );
        }, []);
      });
    });
  }

  refresh() {
    this.getAlerts();
    this.getRules();
  }

  private handleAlerts(alerts: AlertmanagerAlert[]) {
    if (this.canAlertsBeNotified) {
      this.notifyOnAlertChanges(alerts, this.alerts);
    }
    this.activeAlerts = _.reduce<AlertmanagerAlert, number>(
      alerts,
      (result, alert) => (alert.status.state === 'active' ? ++result : result),
      0
    );
    this.activeCriticalAlerts = _.reduce<AlertmanagerAlert, number>(
      alerts,
      (result, alert) =>
        alert.status.state === 'active' && alert.labels.severity === 'critical' ? ++result : result,
      0
    );
    this.activeWarningAlerts = _.reduce<AlertmanagerAlert, number>(
      alerts,
      (result, alert) =>
        alert.status.state === 'active' && alert.labels.severity === 'warning' ? ++result : result,
      0
    );
    this.alerts = alerts
      .reverse()
      .sort((a, b) => a.labels.severity.localeCompare(b.labels.severity));
    this.canAlertsBeNotified = true;
  }

  private notifyOnAlertChanges(alerts: AlertmanagerAlert[], oldAlerts: AlertmanagerAlert[]) {
    const changedAlerts = this.getChangedAlerts(
      this.alertFormatter.convertToCustomAlerts(alerts),
      this.alertFormatter.convertToCustomAlerts(oldAlerts)
    );
    const suppressedFiltered = _.filter(changedAlerts, (alert) => {
      return alert.status !== 'suppressed';
    });
    const notifications = suppressedFiltered.map((alert) =>
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
