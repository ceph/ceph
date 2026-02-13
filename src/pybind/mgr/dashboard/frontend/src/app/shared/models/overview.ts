export type HealthStatus = 'HEALTH_OK' | 'HEALTH_WARN' | 'HEALTH_ERR';

export const HealthIconMap = {
  HEALTH_OK: 'success',
  HEALTH_WARN: 'warningAltFilled',
  HEALTH_ERR: 'error'
};

export const SeverityIconMap = {
  0: 'success',
  1: 'warningAltFilled',
  2: 'error'
};

/** 0 ok, 1 warn, 2 err */
export type Severity = 0 | 1 | 2;

export type Health = {
  message: string;
  title: string;
  icon: string;
};

const WarnAndErrMessage = $localize`There are active alerts and unresolved health warnings.`;

export const HealthMap: Record<HealthStatus, Health> = {
  HEALTH_OK: {
    message: $localize`All core services are running normally`,
    icon: HealthIconMap['HEALTH_OK'],
    title: $localize`Healthy`
  },
  HEALTH_WARN: {
    message: WarnAndErrMessage,
    icon: HealthIconMap['HEALTH_WARN'],
    title: $localize`Warning`
  },
  HEALTH_ERR: {
    message: WarnAndErrMessage,
    icon: HealthIconMap['HEALTH_ERR'],
    title: $localize`Critical`
  }
};

export interface HealthDisplayVM {
  title: string;
  message: string;
  icon: string;
}

export interface HealthCardCheckVM {
  name: string;
  description: string;
  icon: string;
}

export interface HealthCardSubStateVM {
  value: string;
  severity: string;
}

export interface HealthCardVM {
  fsid: string;
  overallSystemSev: string;

  incidents: number;
  checks: HealthCardCheckVM[];

  health: HealthDisplayVM;

  mon: HealthCardSubStateVM;
  mgr: HealthCardSubStateVM;
  osd: HealthCardSubStateVM;
  hosts: HealthCardSubStateVM;
}
