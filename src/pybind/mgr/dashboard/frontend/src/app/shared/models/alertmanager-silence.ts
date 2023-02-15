import { PrometheusRule } from './prometheus-alerts';

export class AlertmanagerSilenceMatcher {
  name: string;
  value: any;
  isRegex: boolean;
}

export class AlertmanagerSilenceMatcherMatch {
  status: string;
  cssClass: string;
}

export class AlertmanagerSilence {
  id?: string;
  matchers: AlertmanagerSilenceMatcher[];
  startsAt: string; // DateStr
  endsAt: string; // DateStr
  updatedAt?: string; // DateStr
  createdBy: string;
  comment: string;
  status?: {
    state: 'expired' | 'active' | 'pending';
  };
  silencedAlerts?: PrometheusRule[];
}
