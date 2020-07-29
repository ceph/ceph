import { Injectable } from '@angular/core';

import * as _ from 'lodash';

import {
  AlertmanagerSilenceMatcher,
  AlertmanagerSilenceMatcherMatch
} from '../models/alertmanager-silence';
import { PrometheusRule } from '../models/prometheus-alerts';

@Injectable({
  providedIn: 'root'
})
export class PrometheusSilenceMatcherService {
  private valueAttributePath = {
    alertname: 'name',
    instance: 'alerts.0.labels.instance',
    job: 'alerts.0.labels.job',
    severity: 'labels.severity'
  };

  singleMatch(
    matcher: AlertmanagerSilenceMatcher,
    rules: PrometheusRule[]
  ): AlertmanagerSilenceMatcherMatch {
    return this.multiMatch([matcher], rules);
  }

  multiMatch(
    matchers: AlertmanagerSilenceMatcher[],
    rules: PrometheusRule[]
  ): AlertmanagerSilenceMatcherMatch {
    if (matchers.some((matcher) => matcher.isRegex)) {
      return undefined;
    }
    matchers.forEach((matcher) => {
      rules = this.getMatchedRules(matcher, rules);
    });
    return this.describeMatch(rules);
  }

  private getMatchedRules(
    matcher: AlertmanagerSilenceMatcher,
    rules: PrometheusRule[]
  ): PrometheusRule[] {
    const attributePath = this.getAttributePath(matcher.name);
    return rules.filter((r) => _.get(r, attributePath) === matcher.value);
  }

  private describeMatch(rules: PrometheusRule[]): AlertmanagerSilenceMatcherMatch {
    let alerts = 0;
    rules.forEach((r) => (alerts += r.alerts.length));
    return {
      status: this.getMatchText(rules.length, alerts),
      cssClass: alerts ? 'has-success' : 'has-warning'
    };
  }

  getAttributePath(name: string): string {
    return this.valueAttributePath[name];
  }

  private getMatchText(rules: number, alerts: number): string {
    const msg = {
      noRule: $localize`Your matcher seems to match no currently defined rule or active alert.`,
      noAlerts: $localize`no active alerts`,
      alert: $localize`1 active alert`,
      alerts: $localize`${alerts} active alerts`,
      rule: $localize`Matches 1 rule`,
      rules: $localize`Matches ${rules} rules`
    };

    const rule = rules > 1 ? msg.rules : msg.rule;
    const alert = alerts ? (alerts > 1 ? msg.alerts : msg.alert) : msg.noAlerts;

    return rules ? $localize`${rule} with ${alert}.` : msg.noRule;
  }
}
