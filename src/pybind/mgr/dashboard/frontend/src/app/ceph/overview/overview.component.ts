import { CommonModule } from '@angular/common';
import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  inject,
  ViewEncapsulation
} from '@angular/core';
import { GridModule, LayoutModule, TilesModule } from 'carbon-components-angular';
import { combineLatest, EMPTY, Observable } from 'rxjs';
import { catchError, exhaustMap, map, shareReplay, startWith } from 'rxjs/operators';

import { HealthService } from '~/app/shared/api/health.service';
import { RefreshIntervalService } from '~/app/shared/services/refresh-interval.service';
import { HealthCheck, HealthSnapshotMap } from '~/app/shared/models/health.interface';
import {
  ACTIVE_CLEAN_CHART_OPTIONS,
  calcActiveCleanSeverityAndReasons,
  getClusterHealth,
  getHealthChecksAndIncidents,
  getResiliencyDisplay,
  HealthCardTabSection,
  HealthCardVM,
  HealthStatus,
  maxSeverity,
  safeDifference,
  SEVERITY,
  Severity,
  SEVERITY_TO_COLOR,
  SeverityIconMap,
  StorageCardVM
} from '~/app/shared/models/overview';

import { OverviewStorageCardComponent } from './storage-card/overview-storage-card.component';
import { OverviewHealthCardComponent } from './health-card/overview-health-card.component';
import { ComponentsModule } from '~/app/shared/components/components.module';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { OverviewAlertsCardComponent } from './alerts-card/overview-alerts-card.component';
import { PerformanceCardComponent } from '~/app/shared/components/performance-card/performance-card.component';
import { DataTableModule } from '~/app/shared/datatable/datatable.module';
import { PipesModule } from '~/app/shared/pipes/pipes.module';
import { OverviewStorageService } from '~/app/shared/api/storage-overview.service';

const SECONDS_PER_HOUR = 3600;
const SECONDS_PER_DAY = 86400;
const TREND_DAYS = 7;

/**
 * Mapper: HealthSnapshotMap -> HealthCardVM
 * Runs only when healthData$ emits.
 */
function buildHealthCardVM(d: HealthSnapshotMap): HealthCardVM {
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

@Component({
  selector: 'cd-overview',
  imports: [
    CommonModule,
    GridModule,
    TilesModule,
    OverviewStorageCardComponent,
    OverviewHealthCardComponent,
    ComponentsModule,
    OverviewAlertsCardComponent,
    PerformanceCardComponent,
    LayoutModule,
    DataTableModule,
    PipesModule
  ],
  standalone: true,
  templateUrl: './overview.component.html',
  styleUrl: './overview.component.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
  encapsulation: ViewEncapsulation.None
})
export class OverviewComponent {
  isHealthPanelOpen = false;
  isPGStatePanelOpen = false;
  activeHealthTab: HealthCardTabSection | null = null;
  tableColumns = [
    { prop: 'count', name: $localize`PGs count` },
    { prop: 'state_name', name: $localize`Status` }
  ];

  private readonly healthService = inject(HealthService);
  private readonly refreshIntervalService = inject(RefreshIntervalService);
  private readonly overviewStorageService = inject(OverviewStorageService);
  private readonly destroyRef = inject(DestroyRef);

  /* HEALTH CARD DATA */
  private readonly healthData$: Observable<HealthSnapshotMap> = this.refreshIntervalObs(() =>
    this.healthService.getHealthSnapshot()
  ).pipe(shareReplay({ bufferSize: 1, refCount: true }));

  readonly healthCardVm$: Observable<HealthCardVM> = this.healthData$.pipe(
    map(buildHealthCardVM),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  /* STORAGE CARD DATA */

  readonly storageVm$ = this.healthData$.pipe(
    map((data: HealthSnapshotMap) => ({
      total: data.pgmap?.bytes_total,
      used: data.pgmap?.bytes_used
    })),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  readonly averageConsumption$ = this.refreshIntervalObs(() =>
    this.overviewStorageService.getAverageConsumption()
  ).pipe(shareReplay({ bufferSize: 1, refCount: true }));

  readonly timeUntilFull$ = this.refreshIntervalObs(() =>
    this.overviewStorageService.getTimeUntilFull()
  ).pipe(shareReplay({ bufferSize: 1, refCount: true }));

  readonly breakdownRawData$ = this.refreshIntervalObs(() =>
    this.overviewStorageService.getStorageBreakdown()
  ).pipe(shareReplay({ bufferSize: 1, refCount: true }));

  readonly capacityThresholds$ = this.refreshIntervalObs(() =>
    this.overviewStorageService.getRawCapacityThresholds()
  ).pipe(shareReplay({ bufferSize: 1, refCount: true }));

  // getTrendData() is already a polling stream through getRangeQueriesData()
  // hence no refresh needed.
  readonly trendData$ = this.overviewStorageService
    .getTrendData(
      Math.floor(Date.now() / 1000) - TREND_DAYS * SECONDS_PER_DAY,
      Math.floor(Date.now() / 1000),
      SECONDS_PER_HOUR
    )
    .pipe(
      map((result) => {
        const values = result?.TOTAL_RAW_USED ?? [];

        return values.map(([ts, val]) => ({
          timestamp: new Date(ts * 1000),
          values: { Used: Number(val) }
        }));
      }),
      shareReplay({ bufferSize: 1, refCount: true })
    );

  readonly storageCardVm$: Observable<StorageCardVM> = combineLatest([
    this.storageVm$,
    this.breakdownRawData$.pipe(startWith(null)),
    this.trendData$.pipe(startWith([])),
    this.averageConsumption$.pipe(startWith('')),
    this.timeUntilFull$.pipe(startWith('')),
    this.capacityThresholds$.pipe(startWith({ osdFullRatio: null, osdNearfullRatio: null }))
  ]).pipe(
    map(
      ([
        storage,
        breakdownRawData,
        consumptionTrendData,
        averageDailyConsumption,
        estimatedTimeUntilFull,
        capacityThresholds
      ]) => {
        const total = storage?.total ?? 0;
        const used = storage?.used ?? 0;
        const [, unit] = this.overviewStorageService.formatBytesForChart(used);

        return {
          totalCapacity: total,
          usedCapacity: used,
          breakdownData: breakdownRawData
            ? this.overviewStorageService.mapStorageChartData(breakdownRawData, unit, used)
            : [],
          isBreakdownLoaded: !!breakdownRawData,
          consumptionTrendData,
          averageDailyConsumption,
          estimatedTimeUntilFull,
          threshold: this.overviewStorageService.getThresholdStatus(
            total,
            storage?.used,
            capacityThresholds.osdNearfullRatio,
            capacityThresholds.osdFullRatio
          )
        };
      }
    ),
    shareReplay({ bufferSize: 1, refCount: true })
  );

  private refreshIntervalObs<T>(fn: () => Observable<T>): Observable<T> {
    return this.refreshIntervalService.intervalData$.pipe(
      exhaustMap(() => fn().pipe(catchError(() => EMPTY))),
      takeUntilDestroyed(this.destroyRef)
    );
  }

  toggleHealthPanel(): void {
    this.isHealthPanelOpen = !this.isHealthPanelOpen;
  }

  togglePGStatesPanel(): void {
    this.isPGStatePanelOpen = !this.isPGStatePanelOpen;
  }
}
