class CommonAlert {
  labels: {
    alertname: string;
    instance: string;
    job: string;
    severity: string;
  };
  annotations: {
    description: string;
    summary: string;
  };
  startsAt: string;
  endsAt: string;
  generatorURL: string;
}

export class PrometheusAlert extends CommonAlert {
  status: {
    state: 'unprocessed' | 'active' | 'suppressed';
    silencedBy: null | string[];
    inhibitedBy: null | string[];
  };
  receivers: string[];
  fingerprint: string;
}

export class PrometheusNotificationAlert extends CommonAlert {
  status: 'firing' | 'resolved';
}

export class PrometheusNotification {
  status: 'firing' | 'resolved';
  groupLabels: object;
  commonAnnotations: object;
  groupKey: string;
  notified: string;
  alerts: PrometheusNotificationAlert[];
  version: string;
  receiver: string;
  externalURL: string;
  commonLabels: {
    severity: string;
  };
}

export class PrometheusCustomAlert {
  status: 'resolved' | 'unprocessed' | 'active' | 'suppressed';
  name: string;
  url: string;
  summary: string;
  fingerprint?: string | boolean;
}
