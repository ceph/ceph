import { TestBed } from '@angular/core/testing';

import { configureTestBed, PrometheusHelper } from '../../../testing/unit-test-helper';
import { PrometheusRule } from '../models/prometheus-alerts';
import { SharedModule } from '../shared.module';
import { PrometheusSilenceMatcherService } from './prometheus-silence-matcher.service';

describe('PrometheusSilenceMatcherService', () => {
  let service: PrometheusSilenceMatcherService;
  let prometheus: PrometheusHelper;
  let rules: PrometheusRule[];

  configureTestBed({
    imports: [SharedModule]
  });

  const addMatcher = (name: string, value: any) => ({
    name: name,
    value: value,
    isRegex: false
  });

  beforeEach(() => {
    prometheus = new PrometheusHelper();
    service = TestBed.inject(PrometheusSilenceMatcherService);
    rules = [
      prometheus.createRule('alert0', 'someSeverity', [prometheus.createAlert('alert0')]),
      prometheus.createRule('alert1', 'someSeverity', []),
      prometheus.createRule('alert2', 'someOtherSeverity', [prometheus.createAlert('alert2')])
    ];
  });

  it('should create', () => {
    expect(service).toBeTruthy();
  });

  describe('test rule matching with one matcher', () => {
    const expectSingleMatch = (
      name: string,
      value: any,
      helpText: string,
      successClass: boolean
    ) => {
      const match = service.singleMatch(addMatcher(name, value), rules);
      expect(match.status).toBe(helpText);
      expect(match.cssClass).toBe(successClass ? 'has-success' : 'has-warning');
    };

    it('should match no rule and no alert', () => {
      expectSingleMatch(
        'alertname',
        'alert',
        'Your matcher seems to match no currently defined rule or active alert.',
        false
      );
    });

    it('should match a rule with no alert', () => {
      expectSingleMatch('alertname', 'alert1', 'Matches 1 rule with no active alerts.', false);
    });

    it('should match a rule and an alert', () => {
      expectSingleMatch('alertname', 'alert0', 'Matches 1 rule with 1 active alert.', true);
    });

    it('should match multiple rules and an alert', () => {
      expectSingleMatch('severity', 'someSeverity', 'Matches 2 rules with 1 active alert.', true);
    });

    it('should match multiple rules and multiple alerts', () => {
      expectSingleMatch('job', 'someJob', 'Matches 2 rules with 2 active alerts.', true);
    });

    it('should return any match if regex is checked', () => {
      const match = service.singleMatch(
        {
          name: 'severity',
          value: 'someSeverity',
          isRegex: true
        },
        rules
      );
      expect(match).toBeFalsy();
    });
  });

  describe('test rule matching with multiple matcher', () => {
    const expectMultiMatch = (matchers: any[], helpText: string, successClass: boolean) => {
      const match = service.multiMatch(matchers, rules);
      expect(match.status).toBe(helpText);
      expect(match.cssClass).toBe(successClass ? 'has-success' : 'has-warning');
    };

    it('should match no rule and no alert', () => {
      expectMultiMatch(
        [addMatcher('alertname', 'alert0'), addMatcher('job', 'ceph')],
        'Your matcher seems to match no currently defined rule or active alert.',
        false
      );
    });

    it('should match a rule with no alert', () => {
      expectMultiMatch(
        [addMatcher('severity', 'someSeverity'), addMatcher('alertname', 'alert1')],
        'Matches 1 rule with no active alerts.',
        false
      );
    });

    it('should match a rule and an alert', () => {
      expectMultiMatch(
        [addMatcher('instance', 'someInstance'), addMatcher('alertname', 'alert0')],
        'Matches 1 rule with 1 active alert.',
        true
      );
    });

    it('should return any match if regex is checked', () => {
      const match = service.multiMatch(
        [
          addMatcher('instance', 'someInstance'),
          {
            name: 'severity',
            value: 'someSeverity',
            isRegex: true
          }
        ],
        rules
      );
      expect(match).toBeFalsy();
    });
  });
});
