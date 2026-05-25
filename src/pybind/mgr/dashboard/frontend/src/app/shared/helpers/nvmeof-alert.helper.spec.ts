import {
  isNvmeofAlert,
  nvmeofAlertQueryParams,
  nvmeofCategoryFilterPredicate
} from './nvmeof-alert.helper';
import { AlertmanagerAlert } from '~/app/shared/models/prometheus-alerts';

describe('nvmeof-alert.helper', () => {
  const alert = (labels: Record<string, string>, state = 'active'): AlertmanagerAlert => {
    const alertLabels: AlertmanagerAlert['labels'] = {
      alertname: labels.alertname ?? '',
      instance: labels.instance ?? '',
      job: labels.job ?? '',
      severity: labels.severity ?? '',
      category: labels.category,
      ...labels
    };

    return {
      labels: alertLabels,
      annotations: { description: '', summary: '' },
      startsAt: new Date().toISOString(),
      endsAt: new Date().toISOString(),
      generatorURL: '',
      status: {
        state: state as AlertmanagerAlert['status']['state'],
        silencedBy: null,
        inhibitedBy: null
      },
      receivers: [],
      fingerprint: 'test-fingerprint',
      alert_count: 1
    };
  };

  describe('isNvmeofAlert', () => {
    it('should match job nvmeof', () => {
      expect(isNvmeofAlert(alert({ job: 'nvmeof', alertname: 'X' }))).toBe(true);
    });

    it('should match known category labels', () => {
      expect(isNvmeofAlert(alert({ category: 'gateway', alertname: 'X' }))).toBe(true);
    });

    it('should not match by alertname without nvme labels', () => {
      expect(isNvmeofAlert(alert({ alertname: 'NVMeoFHighGatewayCPU' }))).toBe(false);
    });

    it('should not match unrelated alerts', () => {
      expect(isNvmeofAlert(alert({ alertname: 'CephDaemonCrash', severity: 'critical' }))).toBe(
        false
      );
    });
  });

  describe('nvmeofCategoryFilterPredicate', () => {
    it('should pass all rows when filter is All', () => {
      expect(nvmeofCategoryFilterPredicate(alert({ category: 'gateway' }), 'All' as any)).toBe(
        true
      );
    });
  });

  describe('nvmeofAlertQueryParams', () => {
    it('should include scope and optional category', () => {
      expect(nvmeofAlertQueryParams('critical')).toEqual({
        severity: 'critical',
        scope: 'nvmeof'
      });
      expect(nvmeofAlertQueryParams('all', 'gateway')).toEqual({
        severity: 'all',
        scope: 'nvmeof',
        category: 'gateway'
      });
    });
  });
});
