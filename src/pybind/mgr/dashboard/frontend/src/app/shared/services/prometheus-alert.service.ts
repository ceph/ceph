import { Injectable } from '@angular/core';

import _ from 'lodash';

import { PrometheusService } from '../api/prometheus.service';
import {
  AlertmanagerAlert,
  PrometheusCustomAlert,
  PrometheusRule,
  GroupAlertmanagerAlert,
  AlertState
} from '../models/prometheus-alerts';
import { PrometheusAlertFormatter } from './prometheus-alert-formatter';
import { BehaviorSubject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class PrometheusAlertService {
  private canAlertsBeNotified = false;
  private rulesSubject = new BehaviorSubject<PrometheusRule[]>([]);
  rules$ = this.rulesSubject.asObservable();
  alerts: AlertmanagerAlert[] = [];
  activeAlerts: number;
  activeCriticalAlerts: number;
  activeWarningAlerts: number;

  constructor(
    private alertFormatter: PrometheusAlertFormatter,
    private prometheusService: PrometheusService
  ) {}

  getGroupedAlerts(clusterFilteredAlerts = false) {
    this.prometheusService.ifAlertmanagerConfigured(() => {
      this.prometheusService.getGroupedAlerts(clusterFilteredAlerts).subscribe(
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
        const rules = groups['groups'].reduce((acc, group) => {
          return acc.concat(
            group.rules.map((rule) => {
              rule.group = group.name;
              return rule;
            })
          );
        }, []);
        this.rulesSubject.next(rules);
      });
    });
  }

  refresh() {
    this.getGroupedAlerts(true);
  }

  private handleAlerts(alertGroups: GroupAlertmanagerAlert[]) {
    const alerts: AlertmanagerAlert[] = alertGroups
      .map((group) => {
        if (!group.alerts.length) return null;
        if (group.alerts.length === 1) return { ...group.alerts[0], alert_count: 1 };
        const hasActive = group.alerts.some(
          (alert: AlertmanagerAlert) => alert.status.state === AlertState.ACTIVE
        );
        const parent = { ...group.alerts[0] };
        if (hasActive) parent.status.state = AlertState.ACTIVE;
        return { ...parent, alert_count: group.alerts.length, subalerts: group.alerts };
      })
      .filter(Boolean) as AlertmanagerAlert[];

    if (this.canAlertsBeNotified) {
      const allSubalerts = alertGroups.flatMap((g) => g.alerts);
      const oldAlerts = this.alerts.flatMap((a) => (a.subalerts ? a.subalerts : a));
      this.notifyOnAlertChanges(allSubalerts, oldAlerts);
    }
    this.activeAlerts = _.reduce<AlertmanagerAlert, number>(
      alerts,
      (result, alert) => (alert.status.state === AlertState.ACTIVE ? ++result : result),
      0
    );
    this.activeCriticalAlerts = _.reduce<AlertmanagerAlert, number>(
      alerts,
      (result, alert) =>
        alert.status.state === AlertState.ACTIVE && alert.labels.severity === 'critical'
          ? ++result
          : result,
      0
    );
    this.activeWarningAlerts = _.reduce<AlertmanagerAlert, number>(
      alerts,
      (result, alert) =>
        alert.status.state === AlertState.ACTIVE && alert.labels.severity === 'warning'
          ? ++result
          : result,
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
      return alert.status !== AlertState.SUPPRESSED;
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
        alert.status = AlertState.RESOLVED;
        return alert;
      }
    );
  }
}
