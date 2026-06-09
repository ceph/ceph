import { AlertmanagerAlert } from '~/app/shared/models/prometheus-alerts';

/** Query param value used by NVMe-oF dashboard links to pre-filter active alerts. */
export const NVMEOF_ALERT_SCOPE = 'nvmeof';

/** Matches monitoring/ceph-mixin NVMe-oF alert rule category labels. */
export const NVMEOF_ALERT_CATEGORIES = new Set([
  'gateway',
  'subsystem',
  'listener',
  'namespace',
  'performance',
  'host'
]);

export const NVMEOF_SCOPE_LABELS = {
  all: $localize`All`,
  nvmeof: $localize`NVMe-oF`
};

export const NVMEOF_CATEGORY_LABELS: Record<string, string> = {
  all: $localize`All`,
  gateway: $localize`Gateway`,
  subsystem: $localize`Subsystem`,
  listener: $localize`Listener`,
  namespace: $localize`Namespace`,
  performance: $localize`Performance`,
  host: $localize`Host`
};

export const NVMEOF_CATEGORY_FILTER_OPTIONS = Object.values(NVMEOF_CATEGORY_LABELS);

export function isNvmeofAlert(alert: AlertmanagerAlert): boolean {
  const labels = alert.labels;
  if (!labels) {
    return false;
  }
  const job = labels.job?.toLowerCase();
  if (job === 'nvme' || job === 'nvmeof') {
    return true;
  }
  if (labels.category && NVMEOF_ALERT_CATEGORIES.has(labels.category)) {
    return true;
  }
  return false;
}

export function nvmeofCategoryFilterPredicate(row: AlertmanagerAlert, value: string): boolean {
  const key =
    Object.entries(NVMEOF_CATEGORY_LABELS).find(([, label]) => label === value)?.[0] ?? 'all';
  if (key === 'all') {
    return true;
  }
  return row.labels?.category === key;
}

export function nvmeofAlertQueryParams(
  severity: string,
  category?: string
): { severity: string; scope: string; category?: string } {
  const params: { severity: string; scope: string; category?: string } = {
    severity,
    scope: NVMEOF_ALERT_SCOPE
  };
  if (category) {
    params.category = category;
  }
  return params;
}
