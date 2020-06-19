import { Injectable } from '@angular/core';

import { I18n } from '@ngx-translate/i18n-polyfill';
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

  constructor(private i18n: I18n) {}

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
      noRule: this.i18n('Your matcher seems to match no currently defined rule or active alert.'),
      noAlerts: this.i18n('no active alerts'),
      alert: this.i18n('1 active alert'),
      alerts: this.i18n('{{n}} active alerts', { n: alerts }),
      rule: this.i18n('Matches 1 rule'),
      rules: this.i18n('Matches {{n}} rules', { n: rules })
    };
    return rules
      ? this.i18n('{{rules}} with {{alerts}}.', {
          rules: rules > 1 ? msg.rules : msg.rule,
          alerts: alerts ? (alerts > 1 ? msg.alerts : msg.alert) : msg.noAlerts
        })
      : msg.noRule;
  }
}
