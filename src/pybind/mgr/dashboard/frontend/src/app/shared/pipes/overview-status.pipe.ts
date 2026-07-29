import { Pipe, PipeTransform } from '@angular/core';

export type OverviewFieldStatusType = 'success' | 'warning' | 'danger' | 'info';
export type OverviewFieldStatus = OverviewFieldStatusType | null | undefined;

export interface OverviewStatusMeta {
  icon: string;
  textClass: string;
}

const OVERVIEW_STATUS_MAP: Record<OverviewFieldStatusType, OverviewStatusMeta> = {
  success: {
    icon: 'checkmarkFilled',
    textClass: 'cd-status-text--success'
  },
  warning: {
    icon: 'warningAltFilled',
    textClass: 'cd-status-text--warning'
  },
  danger: {
    icon: 'warningFilled',
    textClass: 'cd-status-text--danger'
  },
  info: {
    icon: 'informationFilled',
    textClass: 'cd-status-text--info'
  }
};

/* Pipe to transform the overview field status into corresponding icon or text class */
@Pipe({
  name: 'overviewStatus',
  standalone: false
})
export class OverviewStatusPipe implements PipeTransform {
  transform(status: OverviewFieldStatus): OverviewStatusMeta {
    const normalizedStatus: OverviewFieldStatusType =
      status === 'success' || status === 'warning' || status === 'danger' ? status : 'info';

    return OVERVIEW_STATUS_MAP[normalizedStatus];
  }
}
