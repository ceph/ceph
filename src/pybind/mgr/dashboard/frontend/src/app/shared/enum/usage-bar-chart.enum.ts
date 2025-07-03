import { Statuses } from '@carbon/charts-angular';

export const StatusToCssMap: Record<Statuses, string> = {
  [Statuses.SUCCESS]: 'cds-support-success',
  [Statuses.WARNING]: 'cds-support-warning',
  [Statuses.DANGER]: 'cds-support-error'
};
