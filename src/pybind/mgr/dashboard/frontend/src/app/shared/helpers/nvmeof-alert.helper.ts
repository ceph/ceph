import { AlertmanagerAlert } from '~/app/shared/models/prometheus-alerts';

type SeverityFilter = 'all' | 'critical' | 'warning';

type NvmeofAlertLabels = AlertmanagerAlert['labels'] & {
  category?: string;
};

export const isNvmeofAlert = (alert: AlertmanagerAlert): boolean => {
  const labels = alert?.labels as NvmeofAlertLabels | undefined;
  if (!labels) {
    return false;
  }

  const job = labels.job?.toLowerCase();
  if (job === 'nvme' || job === 'nvmeof') {
    return true;
  }

  const alertName = labels.alertname?.toLowerCase() ?? '';
  const category = labels.category?.toLowerCase();

  return alertName.includes('nvme') && !!category;
};

export const nvmeofAlertQueryParams = (
  severity: SeverityFilter = 'all',
  category?: string,
  nvmeofOnly = true
): Record<string, string> => {
  const queryParams: Record<string, string> = { severity };

  if (category) {
    queryParams['category'] = category;
  }

  if (nvmeofOnly) {
    queryParams['nvmeof'] = 'true';
  }

  return queryParams;
};
