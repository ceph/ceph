import { HealthCheck, PgStateCount } from './health.interface';

export type HealthStatus = 'HEALTH_OK' | 'HEALTH_WARN' | 'HEALTH_ERR';

export const HealthIconMap = {
  HEALTH_OK: 'success',
  HEALTH_WARN: 'warningAltFilled',
  HEALTH_ERR: 'error'
};

export const SeverityIconMap = {
  0: 'success',
  1: 'warningAltFilled',
  2: 'error',
  3: 'inProgress'
};

/** 0 ok, 1 warn, 2 err , 3 sync*/
export type Severity = 0 | 1 | 2 | 3;

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

type ResileincyHealthType = {
  title: string;
  description: string;
  icon: string;
};

export interface HealthCardVM {
  fsid: string;
  overallSystemSev: string;

  incidents: number;
  checks: HealthCardCheckVM[];

  clusterHealth: HealthDisplayVM;

  resiliencyHealth: ResileincyHealthType;

  pgs: {
    total: number;
    states: PgStateCount[];
    io: Array<{ label: string; value: number }>;
  };

  mon: HealthCardSubStateVM;
  mgr: HealthCardSubStateVM;
  osd: HealthCardSubStateVM;
  hosts: HealthCardSubStateVM;
}

export type HealthCardTabSection = 'system' | 'hardware' | 'resiliency';

export const SEVERITY = {
  ok: 0 as Severity,
  warn: 1 as Severity,
  err: 2 as Severity,
  sync: 3 as Severity
} as const;

export const RESILIENCY_CHECK = {
  error: ['PG_DAMAGED', 'PG_RECOVERY_FULL'],
  warn: ['PG_DEGRADED', 'PG_AVAILABILITY', 'PG_BACKFILL_FULL']
};

const DATA_RESILIENCY_STATE = {
  ok: 'ok',
  error: 'error',
  warn: 'warn',
  warnDataLoss: 'warnDataLoss',
  progress: 'progress'
};

export const DATA_RESILIENCY = {
  [DATA_RESILIENCY_STATE.ok]: {
    icon: 'success',
    title: $localize`Data is fully replicated and available.`,
    description: $localize`All replicas are in place and I/O is operating normally. No action is required.`
  },
  [DATA_RESILIENCY_STATE.progress]: {
    icon: 'inProgress',
    title: $localize`Data integrity checks in progress`,
    description: $localize`Ceph is running routine consistency checks on stored data and metadata to ensure data integrity. Data remains safe and accessible.`
  },
  [DATA_RESILIENCY_STATE.warn]: {
    icon: 'warning',
    title: $localize`Restoring data redundancy`,
    description: $localize`Some data replicas are missing or not yet in their final location. Ceph is actively rebalancing data to return to a healthy state.`
  },
  [DATA_RESILIENCY_STATE.warnDataLoss]: {
    icon: 'warning',
    title: $localize`Status unavailable for some data`,
    description: $localize`Ceph cannot reliably determine the current state of some data. Availability may be affected.`
  },
  [DATA_RESILIENCY_STATE.error]: {
    icon: 'error',
    title: $localize`Data unavailable or inconsistent, manual intervention required`,
    description: $localize`Some data is currently unavailable or inconsistent. Ceph could not automatically restore these resources, and manual intervention is required to restore data availability and consistency.`
  }
};

export const maxSeverity = (...values: Severity[]): Severity => Math.max(...values) as Severity;

export function getClusterHealth(status: HealthStatus): HealthDisplayVM {
  return HealthMap[status] ?? HealthMap['HEALTH_OK'];
}

export function getResiliencyDisplay(checks: HealthCardCheckVM[] = []): ResileincyHealthType {
  let resileincyState: string = DATA_RESILIENCY_STATE.ok;
  checks.forEach((check) => {
    switch (check?.name) {
      case RESILIENCY_CHECK.error[0]:
      case RESILIENCY_CHECK.error[1]:
        resileincyState = DATA_RESILIENCY_STATE.error;
        break;
      case RESILIENCY_CHECK.warn[0]:
        resileincyState = DATA_RESILIENCY_STATE.warn;
        break;
      case RESILIENCY_CHECK.warn[1]:
        resileincyState = DATA_RESILIENCY_STATE.warnDataLoss;
        break;
    }
  });
  return DATA_RESILIENCY[resileincyState];
}

export function getHealthChecksAndIncidents(checksObj: Record<string, HealthCheck>) {
  const checks: HealthCardCheckVM[] = [];
  let incidents = 0;
  for (const [name, check] of Object.entries(checksObj)) {
    incidents++;
    checks.push({
      name,
      description: check?.summary?.message ?? '',
      icon: HealthIconMap[check?.severity] ?? ''
    });
  }

  return { incidents, checks };
}

export function safeDifference(a: number, b: number): number | null {
  return a != null && b != null ? a - b : null;
}
