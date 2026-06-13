import { ChartTabularData, GaugeChartOptions } from '@carbon/charts-angular';
import { HealthCheck, HealthSnapshotMap, PgStateCount } from './health.interface';
import _ from 'lodash';

// Types
type ResileincyHealthType = {
  title: string;
  description: string;
  icon: string;
  severity: ResiliencyState;
};

type ResiliencyState = typeof DATA_RESILIENCY_STATE[keyof typeof DATA_RESILIENCY_STATE];

type PG_STATES = typeof PG_STATES[number];

type SCRUBBING_STATES = typeof SCRUBBING_STATES[number];

export type TrendPoint = {
  timestamp: Date;
  values: { Used: number };
};

export type BreakdownChartData = { group: string; value: number };

export type CapacityThreshold = 'high' | 'critical' | null;

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

export type HealthStatus = 'HEALTH_OK' | 'HEALTH_WARN' | 'HEALTH_ERR';

export type HealthCardTabSection = 'system' | 'hardware' | 'resiliency';

/** 0 ok, 1 warn, 2 err */
export type Severity = 0 | 1 | 2;

export type Health = {
  message: string;
  title: string;
  icon: string;
};

// Interfaces

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

  clusterHealth: HealthDisplayVM;

  resiliencyHealth: ResileincyHealthType;

  pgs: {
    total: number;
    states: PgStateCount[];
    io: Array<{ label: string; value: number }>;
    activeCleanChartData: ChartTabularData;
    activeCleanChartOptions: GaugeChartOptions;
    activeCleanChartReason: Array<{ state: string; count: number }>;
  };

  mon: HealthCardSubStateVM;
  mgr: HealthCardSubStateVM;
  osd: HealthCardSubStateVM;
  hosts: HealthCardSubStateVM;
}

export interface StorageCardVM {
  totalCapacity: number | null;
  usedCapacity: number | null;
  breakdownData: BreakdownChartData[];
  isBreakdownLoaded: boolean;
  consumptionTrendData: TrendPoint[];
  averageDailyConsumption: string;
  estimatedTimeUntilFull: string;
  threshold: CapacityThreshold;
}

// Constants

const WarnAndErrMessage = $localize`There are active alerts and unresolved health warnings.`;

const DATA_RESILIENCY_STATE = {
  ok: 'ok',
  error: 'error',
  warn: 'warn',
  warnDataLoss: 'warnDataLoss',
  progress: 'progress'
} as const;

const CHECK_TO_STATE: Record<string, ResiliencyState> = {
  PG_DAMAGED: DATA_RESILIENCY_STATE.error,
  PG_RECOVERY_FULL: DATA_RESILIENCY_STATE.error,

  PG_DEGRADED: DATA_RESILIENCY_STATE.warn,
  PG_AVAILABILITY: DATA_RESILIENCY_STATE.warnDataLoss,
  PG_BACKFILL_FULL: DATA_RESILIENCY_STATE.warn
} as const;

const RESILIENCY_PRIORITY: Record<ResiliencyState, number> = {
  ok: 0,
  progress: 1,
  warn: 2,
  warnDataLoss: 3,
  error: 4
};

// Priority: DO NOT CHANGE ORDER HERE
const PG_STATES = [
  // ERROR OR WARN
  'offline',
  'inconsistent',
  'down',
  'stale',
  'degraded',
  'undersized',
  'recovering',
  'recovery_wait',
  'backfilling',
  'backfill_wait',
  'remapped',
  'unknown',
  // PROGRESS
  'deep',
  'scrubbing'
] as const;

// PROGRESS
const SCRUBBING_STATES = ['deep', 'scrubbing'];

const LABELS: Record<string, string> = {
  scrubbing: 'Scrub',
  deep: 'Deep-Scrub'
};

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

export const SEVERITY = {
  ok: 0 as Severity,
  warn: 1 as Severity,
  err: 2 as Severity,
  sync: 3 as Severity
} as const;

export const ACTIVE_CLEAN_CHART_OPTIONS: GaugeChartOptions = {
  resizable: true,
  height: '100px',
  width: '100px',
  gauge: { type: 'full' },
  toolbar: {
    enabled: false
  }
};

export const DATA_RESILIENCY: Record<ResiliencyState, ResileincyHealthType> = {
  [DATA_RESILIENCY_STATE.ok]: {
    icon: 'success',
    title: $localize`Data is fully replicated and available.`,
    description: $localize`All replicas are in place and I/O is operating normally. No action is required.`,
    severity: DATA_RESILIENCY_STATE.ok
  },
  [DATA_RESILIENCY_STATE.progress]: {
    icon: 'inProgress',
    title: $localize`Data integrity checks in progress`,
    description: $localize`Ceph is running routine consistency checks on stored data and metadata to ensure data integrity. Data remains safe and accessible.`,
    severity: DATA_RESILIENCY_STATE.progress
  },
  [DATA_RESILIENCY_STATE.warn]: {
    icon: 'warningAltFilled',
    title: $localize`Restoring data redundancy`,
    description: $localize`Some data replicas are missing or not yet in their final location. Ceph is actively rebalancing data to return to a healthy state.`,
    severity: DATA_RESILIENCY_STATE.warn
  },
  [DATA_RESILIENCY_STATE.warnDataLoss]: {
    icon: 'warningAltFilled',
    title: $localize`Status unavailable for some data`,
    description: $localize`Ceph cannot reliably determine the current state of some data. Availability may be affected.`,
    severity: DATA_RESILIENCY_STATE.warnDataLoss
  },
  [DATA_RESILIENCY_STATE.error]: {
    icon: 'error',
    title: $localize`Data unavailable or inconsistent, manual intervention required`,
    description: $localize`Some data is currently unavailable or inconsistent. Ceph could not automatically restore these resources, and manual intervention is required to restore data availability and consistency.`,
    severity: DATA_RESILIENCY_STATE.error
  }
} as const;

export const SEVERITY_TO_COLOR: Record<ResiliencyState, string> = {
  ok: '#24A148',
  progress: '#24A148',
  warn: '#F1C21B',
  warnDataLoss: '#F1C21B',
  error: '#DA1E28'
};

// Utilities

export const maxSeverity = (...values: Severity[]): Severity => Math.max(...values) as Severity;

export function getClusterHealth(status: HealthStatus): HealthDisplayVM {
  return HealthMap[status] ?? HealthMap['HEALTH_OK'];
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

export function getResiliencyDisplay(
  checks: HealthCardCheckVM[] = [],
  pgStates: PgStateCount[] = []
): ResileincyHealthType {
  let state: ResiliencyState = DATA_RESILIENCY_STATE.ok;

  for (const check of checks) {
    const next = CHECK_TO_STATE[check?.name];
    if (next && RESILIENCY_PRIORITY[next] > RESILIENCY_PRIORITY[state]) state = next;
    if (state === DATA_RESILIENCY_STATE.error) break;
  }

  if (state === DATA_RESILIENCY_STATE.ok) {
    const hasScrubbing = pgStates.some((s) => isScrubbing(s?.state_name ?? ''));
    if (hasScrubbing) state = DATA_RESILIENCY_STATE.progress;
  }

  return DATA_RESILIENCY[state];
}

export function getActiveCleanChartSeverity(
  pgStates: PgStateCount[] = [],
  activeCleanRatio: number
): ResiliencyState {
  if (activeCleanRatio >= 1) return DATA_RESILIENCY_STATE.ok;

  const hasActive = pgStates.some((s) => (s?.state_name ?? '').includes('active'));
  return hasActive ? DATA_RESILIENCY_STATE.warn : DATA_RESILIENCY_STATE.error;
}

function labelOf(key: string) {
  return LABELS[key] ?? key.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
}

function isActiveCleanRow(pgRow: string) {
  // E.g active+clean+remapped
  return pgRow.includes('active') && pgRow.includes('clean');
}

function isScrubbing(pgRow: string) {
  return pgRow.includes('scrubbing') || pgRow.includes('deep');
}

/**
 * If any PG state is active and not clean => Warn
 * If any PG state is not active and not clean -> Error
 *
 * In case above is true, the states contributing to that as per
 * PG_STATES priotity List will be added.
 *
 * If all OKAY. then scrubbing shown (if active)
 */
export function calcActiveCleanSeverityAndReasons(
  pgStates: PgStateCount[] = [],
  totalPg: number
): {
  activeCleanPercent: number;
  severity: ResiliencyState;
  reasons: Array<{ state: string; count: number }>;
} {
  if (totalPg <= 0) {
    return { activeCleanPercent: 0, severity: DATA_RESILIENCY_STATE.ok, reasons: [] };
  }

  const errorWarnCounts = new Map<PG_STATES, number>();
  const scrubbingCounts = new Map<SCRUBBING_STATES, number>();
  let reasonsMap: Map<SCRUBBING_STATES, number> | Map<PG_STATES, number> = errorWarnCounts;
  let severity: ResiliencyState = DATA_RESILIENCY_STATE.ok;
  let activeCleanTotal = 0;
  let hasNotActiveNotClean = false;
  let hasActiveNotClean = false;

  for (const state of pgStates) {
    const stateName = (state?.state_name ?? '').trim();
    const stateCount = state?.count ?? 0;
    const isActive = stateName.includes('active');
    const isClean = stateName.includes('clean');

    if (!isActive && !isClean) hasNotActiveNotClean = true;
    if (isActive && !isClean) hasActiveNotClean = true;

    // If all okay then only scrubbing state is shown
    for (const state of SCRUBBING_STATES) {
      if (stateName.includes(state)) {
        scrubbingCounts.set(state, (scrubbingCounts.get(state) ?? 0) + stateCount);
      }
    }

    // active+clean*: no reasons required hence continuing
    if (isActiveCleanRow(stateName)) {
      activeCleanTotal += stateCount;
      continue;
    }

    // Any PG state that is not active+clean contributes to warning/error reasons
    for (const state of PG_STATES) {
      if (stateName.includes(state)) {
        errorWarnCounts.set(state, (errorWarnCounts.get(state) ?? 0) + stateCount);
        break;
      }
    }
  }

  if (hasNotActiveNotClean) severity = DATA_RESILIENCY_STATE.error;
  else if (hasActiveNotClean) severity = DATA_RESILIENCY_STATE.warn;
  else if (scrubbingCounts.size > 0) {
    severity = DATA_RESILIENCY_STATE.progress;
    reasonsMap = scrubbingCounts;
  }

  const reasons =
    reasonsMap?.size === 0
      ? []
      : [...reasonsMap.entries()]
          .sort((a, b) => b[1] - a[1])
          .map(([state, count]) => ({
            state: labelOf(state),
            count: Number(((count / totalPg) * 100).toFixed(2))
          }));

  const activeCleanPercent = Number(((activeCleanTotal / totalPg) * 100).toFixed(2));

  return { activeCleanPercent, severity, reasons };
}

/**
 * Mapper: HealthSnapshotMap -> HealthCardVM
 * Runs only when healthData$ emits.
 */
export function buildHealthCardVM(d: HealthSnapshotMap): HealthCardVM {
  const checksObj: Record<string, HealthCheck> = d.health?.checks ?? {};
  const clusterHealth = getClusterHealth(d.health.status as HealthStatus);
  const pgStates = d?.pgmap?.pgs_by_state ?? [];
  const totalPg = d?.pgmap?.num_pgs ?? 0;

  const { incidents, checks } = getHealthChecksAndIncidents(checksObj);
  const resiliencyHealth = getResiliencyDisplay(checks, pgStates);
  const {
    activeCleanPercent,
    severity: activeCleanChartSeverity,
    reasons: activeCleanChartReason
  } = calcActiveCleanSeverityAndReasons(pgStates, totalPg);

  // --- System sub-states ---

  // MON
  const monTotal = d.monmap?.num_mons ?? 0;
  const monQuorum = (d.monmap as any)?.quorum?.length ?? 0;
  const monSev: Severity = monQuorum < monTotal ? SEVERITY.warn : SEVERITY.ok;

  // MGR
  const mgrActive = d.mgrmap?.num_active ?? 0;
  const mgrStandby = d.mgrmap?.num_standbys ?? 0;
  const mgrSev: Severity =
    mgrActive < 1 ? SEVERITY.err : mgrStandby < 1 ? SEVERITY.warn : SEVERITY.ok;

  // OSD
  const osdUp = (d.osdmap as any)?.up ?? 0;
  const osdIn = (d.osdmap as any)?.in ?? 0;
  const osdTotal = (d.osdmap as any)?.num_osds ?? 0;
  const osdDown = safeDifference(osdTotal, osdUp);
  const osdOut = safeDifference(osdTotal, osdIn);
  const osdSev: Severity = osdDown > 0 || osdOut > 0 ? SEVERITY.err : SEVERITY.ok;

  // HOSTS
  const hostsTotal = d.num_hosts ?? 0;
  const hostsAvailable = (d as any)?.num_hosts_available ?? 0;
  const hostsSev: Severity = hostsAvailable < hostsTotal ? SEVERITY.warn : SEVERITY.ok;

  // Overall = worst of the subsystem severities.
  const overallSystemSev = maxSeverity(monSev, mgrSev, osdSev, hostsSev);

  return {
    fsid: d.fsid,
    overallSystemSev: SeverityIconMap[overallSystemSev],

    incidents,
    checks,

    pgs: {
      total: totalPg,
      states: pgStates,
      io: [
        { label: $localize`Client write`, value: d?.pgmap?.write_bytes_sec ?? 0 },
        { label: $localize`Client read`, value: d?.pgmap?.read_bytes_sec ?? 0 },
        { label: $localize`Recovery I/O`, value: d?.pgmap?.recovering_bytes_per_sec ?? 0 }
      ],
      activeCleanChartData: [{ group: 'value', value: activeCleanPercent }],
      activeCleanChartOptions: {
        ...ACTIVE_CLEAN_CHART_OPTIONS,
        color: { scale: { value: SEVERITY_TO_COLOR[activeCleanChartSeverity] } }
      },
      activeCleanChartReason
    },

    clusterHealth,
    resiliencyHealth,

    mon: { value: $localize`Quorum: ${monQuorum}/${monTotal}`, severity: SeverityIconMap[monSev] },
    mgr: {
      value: $localize`${mgrActive} active, ${mgrStandby} standby`,
      severity: SeverityIconMap[mgrSev]
    },
    osd: { value: $localize`${osdIn}/${osdUp} in/up`, severity: SeverityIconMap[osdSev] },
    hosts: {
      value: $localize`${hostsAvailable} / ${hostsTotal} available`,
      severity: SeverityIconMap[hostsSev]
    }
  };
}
